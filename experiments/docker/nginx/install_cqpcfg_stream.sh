#!/usr/bin/env bash
set -euo pipefail

PUBLIC_BASE_PORT="${CQPCFG_NGINX_PUBLIC_BASE_PORT:-15555}"
UPSTREAM_HOST="${CQPCFG_NGINX_UPSTREAM_HOST:-127.0.0.1}"
UPSTREAM_BASE_PORT="${CQPCFG_NGINX_UPSTREAM_BASE_PORT:-5555}"
ALLOW_CIDR="${CQPCFG_NGINX_ALLOW_CIDR:-}"
CONFIG_PATH="${CQPCFG_NGINX_CONFIG_PATH:-/etc/nginx/cqpcfg-stream.conf}"
read -r -a SUDO_CMD <<<"${SUDO:-sudo}"

if ! command -v nginx >/dev/null 2>&1; then
  echo "nginx is not installed" >&2
  exit 1
fi

if ! nginx -V 2>&1 | grep -q -- '--with-stream'; then
  echo "nginx was built without stream module support" >&2
  exit 1
fi

allow_block=""
if [[ -n "${ALLOW_CIDR}" ]]; then
  allow_block="        allow ${ALLOW_CIDR};
        deny all;"
fi

tmp_config="$(mktemp)"
trap 'rm -f "${tmp_config}"' EXIT

cat >"${tmp_config}" <<EOF
stream {
    upstream cqpcfg_control {
        server ${UPSTREAM_HOST}:${UPSTREAM_BASE_PORT};
    }

    upstream cqpcfg_batch {
        server ${UPSTREAM_HOST}:$((UPSTREAM_BASE_PORT + 1));
    }

    upstream cqpcfg_role {
        server ${UPSTREAM_HOST}:$((UPSTREAM_BASE_PORT + 2));
    }

    upstream cqpcfg_ack {
        server ${UPSTREAM_HOST}:$((UPSTREAM_BASE_PORT + 3));
    }

    upstream cqpcfg_model {
        server ${UPSTREAM_HOST}:$((UPSTREAM_BASE_PORT + 4));
    }

    server {
        listen ${PUBLIC_BASE_PORT};
${allow_block}
        proxy_connect_timeout 5s;
        proxy_timeout 1h;
        proxy_pass cqpcfg_control;
    }

    server {
        listen $((PUBLIC_BASE_PORT + 1));
${allow_block}
        proxy_connect_timeout 5s;
        proxy_timeout 1h;
        proxy_pass cqpcfg_batch;
    }

    server {
        listen $((PUBLIC_BASE_PORT + 2));
${allow_block}
        proxy_connect_timeout 5s;
        proxy_timeout 1h;
        proxy_pass cqpcfg_role;
    }

    server {
        listen $((PUBLIC_BASE_PORT + 3));
${allow_block}
        proxy_connect_timeout 5s;
        proxy_timeout 1h;
        proxy_pass cqpcfg_ack;
    }

    server {
        listen $((PUBLIC_BASE_PORT + 4));
${allow_block}
        proxy_connect_timeout 5s;
        proxy_timeout 1h;
        proxy_pass cqpcfg_model;
    }
}
EOF

"${SUDO_CMD[@]}" cp "${tmp_config}" "${CONFIG_PATH}"

if ! "${SUDO_CMD[@]}" nginx -T 2>/dev/null | grep -q "include ${CONFIG_PATH};"; then
  "${SUDO_CMD[@]}" cp /etc/nginx/nginx.conf /etc/nginx/nginx.conf.cqpcfg.bak
  printf '\n# CQPCFG ZeroMQ TCP stream proxy\ninclude %s;\n' "${CONFIG_PATH}" \
    | "${SUDO_CMD[@]}" tee -a /etc/nginx/nginx.conf >/dev/null
fi

"${SUDO_CMD[@]}" nginx -t
"${SUDO_CMD[@]}" systemctl reload nginx

echo "installed CQPCFG nginx stream proxy"
echo "  public base   : ${PUBLIC_BASE_PORT}"
echo "  upstream base : ${UPSTREAM_HOST}:${UPSTREAM_BASE_PORT}"
echo "  config        : ${CONFIG_PATH}"
