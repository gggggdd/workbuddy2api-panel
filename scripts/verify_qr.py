"""独立验证上游精简 QR 编码器的可解码性。

验证方式：用 OpenCV 的独立 QR 解码器（与 JS 编码器无任何共享代码）
对渲染出的矩阵图做解码，比对原文。这是端到端的黑盒验证。

输入：qr_mats.json（node 用待验证编码器生成，含 text/n/rows）
输出：每个用例的解码结果与结论
"""
import json
import os
import sys

import cv2
import numpy as np

TMP = os.path.expandvars(r"%TEMP%")
mats = json.load(open(os.path.join(TMP, "qr_mats.json"), encoding="utf-8"))

# 渲染参数：模块像素、静默区（规范要求 >= 4 模块）
SCALE = 8
QUIET = 4

det = cv2.QRCodeDetector()
ok = fail = 0

for case in mats:
    text, n, rows = case["text"], case["n"], case["rows"]
    side = (n + QUIET * 2) * SCALE
    img = np.full((side, side), 255, dtype=np.uint8)  # 白底

    for r in range(n):
        for c in range(n):
            if rows[r][c] == "1":
                y = (r + QUIET) * SCALE
                x = (c + QUIET) * SCALE
                img[y:y + SCALE, x:x + SCALE] = 0  # 黑模块

    decoded, points, _ = det.detectAndDecode(img)
    passed = decoded == text
    if passed:
        ok += 1
        print(f"  PASS  n={n:2d}  {text!r}")
    else:
        fail += 1
        print(f"  FAIL  n={n:2d}  want={text!r}")
        print(f"        got ={decoded!r}")

print()
print(f"结论：{ok} 通过 / {fail} 失败（共 {len(mats)} 个用例）")
sys.exit(1 if fail else 0)
