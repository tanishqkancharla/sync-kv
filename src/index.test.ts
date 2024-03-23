import { describe, expect, it } from "vitest"
import { add } from "./index"

describe("add", () => {
	it("works", () => {
		expect(add(2, 2)).toBe(4)
	})
})
