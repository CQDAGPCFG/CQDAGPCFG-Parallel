#!/usr/bin/env python3
from __future__ import annotations

import os
import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from time import sleep
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import digest_guess, ensure_project_paths
from shared.hash_targets import ExperimentHashTargets

ensure_project_paths()

from cqdagpcfg_parallel import cqdagpcfg
from cqdagpcfg_parallel.runtime import CandidateBatch


_HASHCAT_MODE_BY_ALGORITHM = {
    "md5": "0",
    "sha1": "100",
    "sha256": "1400",
}


def _resolve_consumer_backend(value: str) -> str:
    backend = value.strip().lower()
    aliases = {
        "python": "python",
        "hashcat": "hashcat-stdin",
        "hashcat-stdin": "hashcat-stdin",
        "hashcat-stream": "hashcat-stdin",
        "hashcat-file": "hashcat-staged",
        "hashcat-rss": "hashcat-staged",
        "hashcat-staged": "hashcat-staged",
        "hashcat-auto": "hashcat-stdin",
    }
    try:
        return aliases[backend]
    except KeyError as exc:
        supported = ", ".join(sorted(aliases))
        raise ValueError(f"CQPCFG_CONSUMER_BACKEND must be one of: {supported}") from exc


def _env_float(name: str, default: float | None = None) -> float | None:
    value = os.environ.get(name)
    return default if value is None or value == "" else float(value)


def _env_int(name: str, default: int | None = None) -> int | None:
    value = os.environ.get(name)
    return default if value is None or value == "" else int(value)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@cqdagpcfg.remote(
    env_prefix="CQPCFG",
    connect=os.environ.get("CQPCFG_CONNECT") or None,
    node_id=os.environ.get("CQPCFG_NODE_ID") or None,
    num_cpus=_env_float("CQPCFG_RESOURCE_CPUS", 1.0),
    memory=os.environ.get("CQPCFG_RESOURCE_MEMORY") or None,
    num_gpus=_env_int("CQPCFG_RESOURCE_GPUS", 0),
)
class ExperimentNode:
    """Default CQDAGPCFG generator/consumer node used by the experiment."""

    def __init__(self, job_payload) -> None:
        self.job_payload = job_payload
        self.targets = ExperimentHashTargets(job_payload)
        self.hash_algorithm = str(job_payload.get("algorithm", "sha256"))
        self.hash_delay_seconds = float(os.environ.get("CQPCFG_HASH_DELAY_SECONDS", "0.0"))
        self.min_password_length = int(os.environ.get("CQPCFG_MIN_PASSWORD_LENGTH", "0"))
        self.max_password_length = int(os.environ.get("CQPCFG_MAX_PASSWORD_LENGTH", "0"))
        self.consumer_backend = _resolve_consumer_backend(
            os.environ.get("CQPCFG_CONSUMER_BACKEND", "python"),
        )
        self.drain_only = _env_bool("CQPCFG_DRAIN_ONLY", False)
        self.hashcat = self._build_hashcat_consumer(job_payload)

    def _build_hashcat_consumer(self, job_payload):
        if self.drain_only:
            return None
        if self.consumer_backend == "hashcat-stdin":
            return HashcatStdinBatchConsumer(job_payload, self.targets)
        if self.consumer_backend == "hashcat-staged":
            return HashcatStagedBatchConsumer(job_payload, self.targets)
        return None

    def generate(self, source):
        if not self.min_password_length and not self.max_password_length:
            return source
        return self._length_filter

    def _length_filter(self, guess: str) -> str | None:
        if self.min_password_length and len(guess) < self.min_password_length:
            return None
        if self.max_password_length and len(guess) > self.max_password_length:
            return None
        return guess

    def finalize_consumer(self):
        if self.hashcat is not None:
            return self.hashcat.close()
        return None

    @cqdagpcfg.consumer(close=finalize_consumer)
    def consume(self, batch: CandidateBatch):
        if self.drain_only:
            return []

        if self.hashcat is not None:
            return self.hashcat.consume(batch)

        if not self.targets.by_hash:
            raise RuntimeError(
                "no target hashes configured; set CQPCFG_DRAIN_ONLY=1 only for "
                "protocol-only drain benchmarks"
            )

        outputs = []
        for offset, guess in enumerate(batch.iter_guesses()):
            if self.hash_delay_seconds:
                sleep(self.hash_delay_seconds)
            digest = digest_guess(guess, algorithm=self.hash_algorithm)
            for hit in self.targets.match_digest(digest):
                outputs.append(
                    {
                        "offset": offset,
                        "rank": batch.start_rank + offset,
                        "guess": guess,
                        "backend": "python",
                        **hit,
                    }
                )
        return outputs


class HashcatConsumerBase:
    """Hashcat stdin consumer used for fair Fitcrack comparisons."""

    def __init__(self, job_payload, targets: ExperimentHashTargets) -> None:
        self.algorithm = str(job_payload.get("algorithm", "sha256")).lower()
        self.limit = int(job_payload["limit"])
        self.targets = targets
        self.mode = _HASHCAT_MODE_BY_ALGORITHM.get(self.algorithm)
        if self.mode is None:
            supported = ", ".join(sorted(_HASHCAT_MODE_BY_ALGORITHM))
            raise ValueError(f"Hashcat backend supports only: {supported}")
        executable = os.environ.get("CQPCFG_HASHCAT_PATH", "hashcat")
        resolved = shutil.which(executable)
        if resolved is None:
            raise RuntimeError(f"Hashcat executable not found: {executable}")
        self.executable = resolved
        work_dir = os.environ.get("CQPCFG_HASHCAT_WORK_DIR")
        self._temporary_dir = (
            None
            if work_dir
            else tempfile.TemporaryDirectory(prefix="cqpcfg-hashcat-")
        )
        self.work_dir = Path(work_dir) if work_dir else Path(self._temporary_dir.name)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.hashes_path = self.work_dir / "target_hashes.txt"
        self.outfile_path = self.work_dir / "hashcat_hits.txt"
        self.backend_name = "hashcat"
        self._write_target_hashes(job_payload)
        self.process: subprocess.Popen[str] | None = None
        self._stdout = None
        self._stderr = None
        self._seen_outfile_lines: set[str] = set()
        self._finalized = False

    def _write_target_hashes(self, job_payload) -> None:
        target_hashes = sorted(
            {
                *(str(target["hash"]) for target in job_payload["targets"]),
                *(str(digest) for digest in job_payload.get("decoy_hashes", ())),
            }
        )
        if not target_hashes:
            raise RuntimeError(
                "Hashcat backend requires target hashes; use CQPCFG_DRAIN_ONLY=1 "
                "only for protocol-only drain benchmarks"
            )
        self.hashes_path.write_text("\n".join(target_hashes) + "\n", encoding="utf-8")

    def _hashcat_common_args(self) -> list[str]:
        device_types = os.environ.get("CQPCFG_HASHCAT_DEVICE_TYPES", "1")
        command = [
            self.executable,
            "-m",
            self.mode,
            "-a",
            "0",
            str(self.hashes_path),
            "--potfile-disable",
            "--outfile",
            str(self.outfile_path),
            "--outfile-format",
            "1,2",
            "--keep-guessing",
            "--session",
            f"cqpcfg-{os.getpid()}-{id(self)}",
            "-D",
            device_types,
            "--quiet",
        ]
        if device_types.strip() == "1" and not _env_bool(
            "CQPCFG_HASHCAT_ENABLE_GPU_PROBING",
            False,
        ):
            command.extend(
                [
                    "--backend-ignore-cuda",
                    "--backend-ignore-hip",
                ]
            )
        return command

    def _start_stdin_process(self) -> subprocess.Popen:
        command = self._hashcat_common_args()
        extra_args = os.environ.get("CQPCFG_HASHCAT_EXTRA_ARGS")
        if extra_args:
            command.extend(extra_args.split())
        self._stdout = (self.work_dir / "hashcat.stdout.log").open("wb")
        self._stderr = (self.work_dir / "hashcat.stderr.log").open("wb")
        return subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=self._stdout,
            stderr=self._stderr,
        )

    def _run_file_process(self, wordlist_paths: tuple[Path, ...]) -> int:
        if not wordlist_paths:
            return 1
        command = self._hashcat_common_args()
        command[6:6] = [str(path) for path in wordlist_paths]
        extra_args = os.environ.get("CQPCFG_HASHCAT_EXTRA_ARGS")
        if extra_args:
            command.extend(extra_args.split())
        self._stdout = (self.work_dir / "hashcat.stdout.log").open("w", encoding="utf-8")
        self._stderr = (self.work_dir / "hashcat.stderr.log").open("w", encoding="utf-8")
        try:
            completed = subprocess.run(
                command,
                stdout=self._stdout,
                stderr=self._stderr,
                text=True,
                encoding="utf-8",
                check=False,
            )
            return completed.returncode
        finally:
            self._close_logs()

    def _finalize(self) -> None:
        if self._finalized:
            return
        self._finalized = True
        if self.process is None:
            return
        if self.process.stdin is not None:
            self.process.stdin.close()
        timeout = float(os.environ.get("CQPCFG_HASHCAT_FINALIZE_TIMEOUT_SECONDS", "180"))
        try:
            return_code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.process.kill()
            raise RuntimeError("Hashcat did not finish before finalize timeout") from exc
        self._close_logs()
        if return_code not in {0, 1}:
            stderr_tail = self._hashcat_stderr_tail()
            raise RuntimeError(f"Hashcat exited with code {return_code}: {stderr_tail}")

    def _close_logs(self) -> None:
        for handle in (self._stdout, self._stderr):
            if handle is not None and not handle.closed:
                handle.close()

    def _hashcat_stderr_tail(self) -> str:
        stderr_path = self.work_dir / "hashcat.stderr.log"
        if not stderr_path.exists():
            return ""
        return stderr_path.read_text(encoding="utf-8", errors="replace")[-1000:]

    def _collect_outputs(self, batch_id: int) -> tuple[dict, ...]:
        if not self.outfile_path.exists():
            return ()
        outputs = []
        for line in self.outfile_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line or line in self._seen_outfile_lines:
                continue
            self._seen_outfile_lines.add(line)
            digest, separator, guess = line.partition(":")
            if not separator:
                continue
            for target in self.targets.by_hash.get(digest, ()):
                outputs.append(
                    {
                        "backend": self.backend_name,
                        "batch_id": batch_id,
                        "rank": int(target["rank"]),
                        "target_rank": int(target["rank"]),
                        "guess": guess,
                        "hash": digest,
                    }
                )
        return tuple(outputs)

    def close(self) -> tuple[dict, ...]:
        self._finalize()
        return self._collect_outputs(batch_id=-1)

    def _local_artifact_path(self, uri: str) -> Path | None:
        parsed = urlparse(uri)
        if parsed.scheme == "file":
            return Path(unquote(parsed.path))
        if parsed.scheme == "":
            return Path(uri)
        return None

    def _open_artifact_binary(self, uri: str):
        parsed = urlparse(uri)
        if parsed.scheme in {"", "file"}:
            path = Path(unquote(parsed.path if parsed.scheme else uri))
            return path.open("rb")
        if parsed.scheme in {"http", "https"}:
            return urlopen(uri, timeout=30)
        raise RuntimeError(f"unsupported candidate artifact URI scheme: {parsed.scheme}")

    def _file_sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()


class HashcatStdinBatchConsumer(HashcatConsumerBase):
    """Stream candidate batches directly into Hashcat stdin."""

    def __init__(self, job_payload, targets: ExperimentHashTargets) -> None:
        super().__init__(job_payload, targets)
        self.backend_name = "hashcat-stdin"
        self._verify_artifacts = (
            os.environ.get("CQPCFG_HASHCAT_STDIN_VERIFY_ARTIFACTS", "")
            .strip()
            .lower()
            in {"1", "true", "yes", "on"}
        )

    def consume(self, batch: CandidateBatch) -> tuple[dict, ...]:
        if self._finalized:
            return ()
        if self.process is None:
            self.process = self._start_stdin_process()
        assert self.process.stdin is not None
        if batch.is_artifact:
            self._write_artifact_to_stdin(batch)
        else:
            for guess in batch.iter_guesses():
                self.process.stdin.write(guess.encode("utf-8"))
                self.process.stdin.write(b"\n")
        self.process.stdin.flush()
        if batch.end_rank >= self.limit:
            self._finalize()
        return self._collect_outputs(batch.batch_id)

    def _write_artifact_to_stdin(self, batch: CandidateBatch) -> None:
        if batch.artifact_format not in {None, "guess-lines-v1"}:
            raise RuntimeError(f"unsupported candidate artifact format: {batch.artifact_format}")
        if batch.artifact_uri is None:
            raise RuntimeError("candidate artifact batch is missing uri")
        if self._verify_artifacts:
            local_path = self._local_artifact_path(batch.artifact_uri)
            if local_path is None:
                raise RuntimeError("stdin artifact verification requires a local artifact URI")
            actual = self._file_sha256(local_path)
            if actual != batch.artifact_sha256:
                raise RuntimeError(
                    "candidate artifact sha256 mismatch: "
                    f"{actual} != {batch.artifact_sha256}"
                )
        with self._open_artifact_binary(batch.artifact_uri) as handle:
            shutil.copyfileobj(handle, self.process.stdin)


class HashcatStagedBatchConsumer(HashcatConsumerBase):
    """Stage ordered batches on disk, then run Hashcat after generation drains.

    This is the RSS-oriented Hashcat policy.  It avoids overlapping the
    generator/tracker memory footprint with Hashcat's OpenCL runtime footprint.
    """

    def __init__(self, job_payload, targets: ExperimentHashTargets) -> None:
        super().__init__(job_payload, targets)
        self.backend_name = "hashcat-staged"
        self._wordlist_paths: list[Path] = []
        self._verify_artifacts = (
            os.environ.get("CQPCFG_HASHCAT_STAGED_VERIFY_ARTIFACTS", "")
            .strip()
            .lower()
            in {"1", "true", "yes", "on"}
        )

    def consume(self, batch: CandidateBatch) -> tuple[dict, ...]:
        if self._finalized:
            return ()
        if batch.is_artifact:
            self._stage_artifact(batch)
        else:
            self._stage_records(batch)
        if batch.end_rank < self.limit:
            return ()
        self._finalized = True
        return_code = self._run_file_process(tuple(self._wordlist_paths))
        if return_code not in {0, 1}:
            stderr_tail = self._hashcat_stderr_tail()
            raise RuntimeError(f"Hashcat staged backend exited with code {return_code}: {stderr_tail}")
        return self._collect_outputs(batch.batch_id)

    def _stage_records(self, batch: CandidateBatch) -> None:
        path = self.work_dir / f"candidate-records-{batch.batch_id:08d}.txt"
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for record in batch.records:
                handle.write(record.guess)
                handle.write("\n")
        self._wordlist_paths.append(path)

    def _stage_artifact(self, batch: CandidateBatch) -> None:
        if batch.artifact_format not in {None, "guess-lines-v1"}:
            raise RuntimeError(f"unsupported candidate artifact format: {batch.artifact_format}")
        if batch.artifact_uri is None or batch.artifact_sha256 is None:
            raise RuntimeError("candidate artifact batch is missing uri or sha256")
        staged_path = self.work_dir / f"candidate-artifact-{batch.batch_id:08d}.txt"
        source_path = self._local_artifact_path(batch.artifact_uri)
        if source_path is not None:
            try:
                os.link(source_path, staged_path)
            except OSError:
                shutil.copyfile(source_path, staged_path)
        else:
            self._copy_remote_artifact(batch.artifact_uri, staged_path)
        if self._verify_artifacts:
            actual = self._file_sha256(staged_path)
            if actual != batch.artifact_sha256:
                raise RuntimeError(
                    "candidate artifact sha256 mismatch: "
                    f"{actual} != {batch.artifact_sha256}"
                )
        self._wordlist_paths.append(staged_path)

    def _copy_remote_artifact(self, uri: str, target_path: Path) -> None:
        with self._open_artifact(uri) as source, target_path.open("wb") as target:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                target.write(chunk)

    def _local_artifact_path(self, uri: str) -> Path | None:
        return super()._local_artifact_path(uri)

    def _file_sha256(self, path: Path) -> str:
        return super()._file_sha256(path)

    def _open_artifact(self, uri: str):
        parsed = urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            return urlopen(uri, timeout=30)
        if parsed.scheme == "file":
            return Path(unquote(parsed.path)).open("rb")
        if parsed.scheme == "":
            return Path(uri).open("rb")
        raise RuntimeError(f"unsupported candidate artifact URI scheme: {parsed.scheme}")


def main() -> None:
    ExperimentNode.remote()


if __name__ == "__main__":
    main()
