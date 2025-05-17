const commonjs = require('@rollup/plugin-commonjs');
const resolve = require('@rollup/plugin-node-resolve');
const terser = require('@rollup/plugin-terser');

module.exports = {
  input: 'esm.js',
  output: {
    file: 'writable-consumable-stream.min.js',
    format: 'es'
  },
  plugins: [
    commonjs(),
    resolve({
      preferBuiltins: false,
      browser: true
    }),
    terser()
  ]
};
