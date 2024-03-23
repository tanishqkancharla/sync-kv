#! tsx

import { Extractor } from "@microsoft/api-extractor"
import c from "ansi-colors"
import assert from "assert/strict"
import * as esbuild from "esbuild"
import fs from "fs"
import typescript from "typescript"

const packageJson = JSON.parse(fs.readFileSync("./package.json").toString())

const {
	name,
	source: entryFile,
	main: cjsOutFile,
	module: esmOutFile,
	peerDependencies = {},
	dependencies = {},
} = packageJson

assert(entryFile, "`source` (entry point) not specified in package.json")
assert(cjsOutFile, "`main` (cjs out file) not specified in package.json")
assert(esmOutFile, "`module` (esm out file) not specified in package.json")

async function main() {
	await fs.promises.rm("dist", { recursive: true, force: true })
	await fs.promises.mkdir("dist")

	await runEsbuild()
	runTypeScript()
	runApiExtractor()

	await Promise.all([
		fs.promises.rm("dist/types", { recursive: true }),
		fs.promises.rm(`dist/${name}.d.ts`),
	])
}

const sharedConfig: esbuild.BuildOptions = {
	entryPoints: [entryFile],
	minify: false,
	bundle: true,
	platform: "browser",
	sourcemap: "linked",
	sourcesContent: false,
	external: Object.keys({ ...peerDependencies, ...dependencies }),
}

function runEsbuild() {
	return Promise.all([
		esbuild
			.build({ ...sharedConfig, format: "esm", outfile: esmOutFile })
			.then((r) => {
				console.log(`Wrote ${esmOutFile}`)
				return r
			}),
		esbuild
			.build({ ...sharedConfig, format: "cjs", outfile: cjsOutFile })
			.then((r) => {
				console.log(`Wrote ${cjsOutFile}`)
				return r
			}),
	])
}

function runTypeScript() {
	const tsConfig = typescript.readConfigFile(
		"./tsconfig.json",
		typescript.sys.readFile
	)
	const tsConfigParseResult = typescript.parseJsonConfigFileContent(
		tsConfig.config,
		typescript.sys,
		"./"
	)
	const program = typescript.createProgram(
		tsConfigParseResult.fileNames,
		tsConfigParseResult.options
	)
	return program.emit()
}

function runApiExtractor() {
	const result = Extractor.loadConfigAndInvoke("scripts/api-extractor.json", {
		typescriptCompilerFolder: "node_modules/typescript",
		showVerboseMessages: true,
	})
	assert(result.succeeded, "API Extractor failed")
	console.log(c.green("API Extractor was successful"))
	return result
}

main().catch((e) => {
	console.error("build failed", e)
	process.exit(1)
})
