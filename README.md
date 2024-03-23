# npm-ts-boilerplate
A boilerplate for typescript packages for npm. [Esbuild](https://esbuild.github.io), [Api Extractor](https://api-extractor.com), [Vitest](https://vitest.dev).

```sh
# Build into dist folder
npm run build

# Check types and api-extractor
npm run check

npm run test

# Build and publish to NPM (make sure the version is bumped)
npm run release

# Print the bundlesize of the minified package and exit
npm run bundlesize
```

I've been iterating on this for a while. Here are the interesting bits:

* Packages are bundled into `index.cjs` (for CommonJS) and `index.js` (for ESM).
* Types are all bundled into `index.d.ts`
* Sourcemaps for bundles point to `src` directory. One could argue the bundles are readable by themselves (since they're not minified), but the source map files are small, and reading in Typescript vs. Javascript can help debug (you know the type of each variable).
* Only `src` and `dist` folders are published to NPM.
* Tests are in `*.test.ts` files, run by Vitest
* API Extractor checks for you that everything you meant to export is exported and everything that was not meant to be exported isn't.

The reason I don't just use Parcel (my preferred build system) is because of:
* https://github.com/parcel-bundler/parcel/issues/7951
* Couldn't find a great way to integrate API extractor
* Parcel mangles names, and I didn't know how to turn that off.
