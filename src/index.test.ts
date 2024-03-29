import { describe, expect, it } from "vitest"
import {
	Database,
	SyncKVClient,
	SyncKVServer,
	SyncKVServerApi,
	mutation,
	tupleLogger,
} from "./SyncKV"

// Read value from server

describe("SyncKV", () => {
	const mutators = {
		add: mutation((tx, x: number) => {
			const value = tx.get("value") ?? 0
			tx.set("value", value + x)
		}),
	}

	function createServer() {
		return new SyncKVServer(mutators)
	}

	async function createClient(server: SyncKVServer) {
		const client = new SyncKVClient({ server, mutators })
		server.connectToClient(client)
		return client
	}

	it("works", async () => {
		const server = createServer()
		const client = await createClient(server)

		client.mutate.add(2)

		expect(server.get("value")).toBe(2)
	})

	it("works with multiple mutations", async () => {
		const server = createServer()
		const client = await createClient(server)

		client.mutate.add(2)
		client.mutate.add(3)

		expect(server.get("value")).toBe(5)
	})

	it("works with multiple clients", async () => {
		const server = createServer()
		const client1 = await createClient(server)
		const client2 = await createClient(server)

		client1.mutate.add(2)
		client2.mutate.add(3)

		expect(server.get("value")).toBe(5)
	})

	it("works with multiple clients and multiple mutations", async () => {
		const server = createServer()
		const client1 = await createClient(server)
		const client2 = await createClient(server)

		client1.mutate.add(2)
		client2.mutate.add(3)
		client1.mutate.add(4)
		client2.mutate.add(5)

		expect(server.get("value")).toBe(14)
	})

	it("Propagates changes to other clients", async () => {
		const server = createServer()
		const client1 = await createClient(server)
		const client2 = await createClient(server)

		client2.mutate.add(3)

		await wait(1)

		expect(client1.get("value")).toBe(3)
	})

	it("Propagates changes to clients with multiple mutations", async () => {
		const server = createServer()
		const client1 = await createClient(server)
		const client2 = await createClient(server)

		client2.mutate.add(3)
		client2.mutate.add(4)

		await wait(1)

		expect(client1.get("value")).toBe(7)
	})

	it("Client fires reactive subscriptions on local mutation", async () => {
		const server = createServer()
		const client = await createClient(server)
		let value = 0

		client.watch("value", (v) => {
			value = v
		})

		client.mutate.add(3)

		await wait(1)

		expect(value).toBe(3)
	})

	it("Client fires reactive subscriptions on remote mutation", async () => {
		const server = createServer()
		const client1 = await createClient(server)
		const client2 = await createClient(server)

		let value = 0
		client1.watch("value", (v) => {
			value = v
		})

		client2.mutate.add(3)

		await wait(1)

		expect(value).toBe(3)
	})

	it.skip("Works if the network is slow", async () => {})
	it.skip("Keeps optimistic changes until server confirms", async () => {})
	it.skip("Handles network error when pulling changes from server", async () => {})
	it.skip("Handles network error when pushing changes to server", async () => {})

	describe("Todo list schema", () => {
		const mutators = {
			addTodo: mutation((tx, text: string) => {
				const todos = tx.get("todos") ?? []
				tx.set("todos", [...todos, { text, done: false }])
			}),
			toggleTodo: mutation((tx, index: number) => {
				const todos = tx.get("todos") ?? []
				todos[index].done = !todos[index].done
				tx.set("todos", todos)
			}),
		}

		function createServer(
			initialState?: Database,
			name?: string
		): SyncKVServerApi {
			const server = new SyncKVServer(
				mutators,
				initialState,
				name ? tupleLogger([name]) : undefined
			)

			return {
				connectToClient(client) {
					return server.connectToClient(client)
				},
				get(key) {
					return server.get(key)
				},
				pull(clientId, cookie) {
					return server
						.pull(clientId, cookie)
						.then((value) => JSON.parse(JSON.stringify(value)))
				},
				push(clientId, mutations) {
					// Structured clone here is to simulate the network
					// Otherwise we'd be passing by reference
					return server.push(clientId, JSON.parse(JSON.stringify(mutations)))
				},
			}
		}

		async function createClient(server: SyncKVServerApi, name?: string) {
			const client = new SyncKVClient({
				server,
				mutators,
				logger: name ? tupleLogger([name]) : undefined,
			})
			server.connectToClient(client)
			// Wait for sync
			await wait(1)
			return client
		}

		it("works", async () => {
			const server = createServer()
			const client = await createClient(server)

			client.mutate.addTodo("Buy milk")

			expect(server.get("todos")).toEqual([{ text: "Buy milk", done: false }])
		})

		it("works with multiple mutations", async () => {
			const server = createServer()
			const client = await createClient(server)

			client.mutate.addTodo("Buy milk")
			client.mutate.addTodo("Buy eggs")

			expect(server.get("todos")).toEqual([
				{ text: "Buy milk", done: false },
				{ text: "Buy eggs", done: false },
			])
		})

		it("works with multiple clients", async () => {
			const server = createServer()
			const client1 = await createClient(server)
			const client2 = await createClient(server)

			client1.mutate.addTodo("Buy milk")
			client2.mutate.addTodo("Buy eggs")

			expect(server.get("todos")).toEqual([
				{ text: "Buy milk", done: false },
				{ text: "Buy eggs", done: false },
			])
		})

		it("Toggle one todo while adding a different one", async () => {
			const server = createServer({
				todos: [{ text: "Buy milk", done: false }],
			})
			const client1 = await createClient(server)
			const client2 = await createClient(server)

			client1.mutate.toggleTodo(0)
			client2.mutate.addTodo("Buy eggs")
			await wait(1)

			const expectedTodos = [
				{ text: "Buy milk", done: true },
				{ text: "Buy eggs", done: false },
			]

			expect(server.get("todos")).toEqual(expectedTodos)
			expect(client1.get("todos")).toEqual(expectedTodos)
			expect(client2.get("todos")).toEqual(expectedTodos)
		})

		it("Subscribes to key that gets added by other client", async () => {
			const server = createServer({
				todos: [{ text: "Buy milk", done: false }],
			})

			const client1 = await createClient(server)
			const client2 = await createClient(server)

			client2.mutate.toggleTodo(0)

			await wait(1)

			expect(client1.get("todos")).toEqual([{ text: "Buy milk", done: true }])
		})
	})
})

async function wait(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms))
}
