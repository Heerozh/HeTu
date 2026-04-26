import { nodeResolve } from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";

// 把第三方 crypto / 序列化 / 压缩 /  ws 全部声明为 external，
// 让最终消费者来负责依赖加载（与典型 npm 库做法一致）。
// 这样浏览器/Node.js 都能正常 import，而我们的 dist 文件保持轻量。
const external = [
  "ws",
  "msgpackr",
  "pako",
  "libsodium-wrappers-sumo",
  "node:module",
];

export default [
  {
    input: "src/index.js",
    external,
    output: [
      {
        file: "dist/hetu-client.mjs",
        format: "esm",
        sourcemap: true,
      },
      {
        file: "dist/hetu-client.cjs",
        format: "cjs",
        sourcemap: true,
        exports: "named",
      },
    ],
    plugins: [
      nodeResolve({ preferBuiltins: true, browser: false }),
      commonjs(),
    ],
  },
];
