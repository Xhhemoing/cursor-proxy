#!/usr/bin/env bash
# Read-only probe: does the CURRENT 8800 gateway forward image_url parts to the model?
# Uses a tiny generated PNG (solid red 64x64) and asks the model what it sees.
set -e
KEY=$(python3 -c "
import json
d=json.load(open('/home/ubuntu/.local/share/cursor-fast-proxy-rs/cards.json'))
print([c['card_key'] for c in d.get('cards',[]) if not c.get('disabled')][0])")

# 64x64 solid red PNG, base64
PNG=$(python3 -c "
import base64, struct, zlib
w=h=64
raw=b''.join(b'\x00'+b'\xff\x00\x00'*w for _ in range(h))
def chunk(t, d):
    c=struct.pack('>I', len(d))+t+d
    return c+struct.pack('>I', zlib.crc32(t+d)&0xffffffff)
png=b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR', struct.pack('>IIBBBBB', w,h,8,2,0,0,0))+chunk(b'IDAT', zlib.compress(raw))+chunk(b'IEND', b'')
print(base64.b64encode(png).decode())")

MODEL="${1:-grok-4.6}"
echo "=== model=$MODEL ==="
curl -sS -m 90 http://127.0.0.1:8800/v1/chat/completions \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d "{\"model\":\"$MODEL\",\"stream\":false,\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"What color is this image? Answer with one word.\"},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,$PNG\"}}]}]}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); m=d.get('choices',[{}])[0].get('message',{}); print('CONTENT:', m.get('content')); print('ERR:', d.get('error'))"
