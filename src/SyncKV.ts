export type Database = Record<string, any>
type Patch = Record<string, any>

// TODO: deletes
type TransactionApi = {
	get: (key: string) => any
	set: (key: string, value: any) => void
}

type AnyMutator = (tx: TransactionApi, ...args: any[]) => void
type AnyMutators = {
	[key: string]: AnyMutator
}

type Mutation = {
	mutationId: string
	key: string
	args: any[]
}

type ExceptFirst<T extends any[]> = T extends [any, ...infer U] ? U : never

type MutateApi<Mutators extends AnyMutators> = {
	[key in keyof Mutators]: (
		...args: ExceptFirst<Parameters<Mutators[key]>>
	) => void
}

type OptimisticMutationRecord = {
	mutationId: string
	patch: Patch
	key: string
	args: any[]
}

type Cookie = number

export type SyncKVServerApi = {
	pull: (
		clientId: string,
		cookie?: Cookie
	) => Promise<{ cookie: Cookie; patch: Patch; lastMutationId?: string }>

	push: (clientId: string, mutations: Mutation[]) => Promise<void>

	connectToClient: (client: SyncKVClientApi) => () => void

	get: (key: string) => any
}

export type SyncKVClientApi = {
	poke: () => void
}

type SyncKVClientProps<Mutators extends AnyMutators> = {
	mutators: Mutators
	server: SyncKVServerApi
	logger?: TupleLoggerApi
}

export class SyncKVClient<Mutators extends AnyMutators>
	implements SyncKVClientApi
{
	private mutators: Mutators
	private server: SyncKVServerApi
	private logger?: TupleLoggerApi

	private db: Database = {}
	private clientId: string = randomId()
	private optimisticMutations: OptimisticMutationRecord[] = []
	private cookie?: Cookie
	private subscriptions: Map<string, ((value: any) => void)[]> = new Map()
	private pullQueued = false

	constructor({ mutators, server, logger }: SyncKVClientProps<Mutators>) {
		this.mutators = mutators
		this.server = server
		this.logger = logger

		this.logger?.log("Mounted client", this.clientId)

		// Initialize
		this.server.pull(this.clientId).then(this.onPull)
	}

	mutate: MutateApi<Mutators> = new Proxy(
		{},
		{
			get: (_, key: string) => {
				return (...args: any[]) => {
					this.logger?.log("Mutating", key, args)
					const tx = new Transaction(this.db)
					this.mutators[key](tx, ...args)
					const mutation: Mutation = {
						mutationId: tx.mutationId,
						key,
						args,
					}
					const record = {
						...mutation,
						patch: tx.patch,
					}
					this.logger?.log("Optimistic mutation", record)
					this.optimisticMutations.push(record)
					// Emit to subscriptions
					for (const key of Object.keys(tx.patch)) {
						this.emit(key)
					}

					// Push the mutation to server
					// TODO: batch mutations, handle errors, etc.
					this.logger?.log("Pushing mutation", mutation)
					this.server.push(this.clientId, [mutation])
				}
			},
		}
	) as MutateApi<Mutators>

	watch(key: string, callback: (value: any) => void): () => void {
		const existing = this.subscriptions.get(key)
		if (existing) {
			existing.push(callback)
		} else {
			this.subscriptions.set(key, [callback])
		}

		return () => {
			const existing = this.subscriptions.get(key)
			if (!existing) return
			const index = existing.indexOf(callback)
			if (index === -1) return

			existing.splice(index, 1)
		}
	}

	private emit(key: string) {
		const value = this.get(key)
		const subscriptions = this.subscriptions.get(key)
		if (!subscriptions) return
		for (const callback of subscriptions) {
			callback(value)
		}
	}

	get(key: string): any {
		// Check optimistic patches first
		for (const mutation of this.optimisticMutations) {
			if (key in mutation.patch) {
				return mutation.patch[key]
			}
		}

		return this.db[key]
	}

	poke() {
		this.logger?.log("Got poked", this.clientId)
		if (this.cookie === undefined) {
			// Initial pull hasn't completed yet
			// this.pullQueued = true
			return
		}
		this.server.pull(this.clientId, this.cookie).then(this.onPull)
	}

	onPull = ({
		cookie,
		patch,
		lastMutationId,
	}: {
		cookie: Cookie
		patch: Patch
		lastMutationId?: string
	}) => {
		this.logger?.log("Received pull", JSON.stringify(patch, null, 2), {
			cookie,
			lastMutationId,
		})
		if (!lastMutationId) {
			// No last mutation id means this is the intial pull
			// We can just apply the patch directly
			Object.assign(this.db, patch)
			this.cookie = cookie
			for (const key of Object.keys(patch)) {
				this.emit(key)
			}
			if (this.pullQueued) {
				this.pullQueued = false
				this.poke()
			}
			return
		}

		// Find index in optimistic patches matching the last mutation id the server
		// applied
		const index = this.optimisticMutations.findIndex(
			(mutation) => mutation.mutationId === lastMutationId
		)
		if (index === -1) {
			this.logger?.log("Optimistic mutations", this.optimisticMutations)
			// Something went wrong: the server said it applied a mutation that we don't have
			return
		}

		// Apply server patch
		Object.assign(this.db, patch)

		// Create a patch to emit to subscriptions
		const patchToEmit = patch

		// Re-base the mutations above that mutation id onto the new state
		for (let i = index + 1; i < this.optimisticMutations.length; i++) {
			const { args, key } = this.optimisticMutations[i]
			const tx = new Transaction(this.db)
			// TODO: surface the potential merge conflict to the mutator
			this.mutators[key](tx, ...args)
			// We have to use the same mutation id so we can detect if the server has applied it
			this.optimisticMutations[i].patch = tx.patch

			Object.assign(patchToEmit, tx.patch)
		}

		// Throw away the mutations that were acked by the server
		this.optimisticMutations = this.optimisticMutations.slice(index + 1)

		this.logger?.log("client db", this.db, this.optimisticMutations)

		// Update cookie
		this.cookie = cookie

		// Emit subscriptions with the new state
		for (const key of Object.keys(patchToEmit)) {
			this.emit(key)
		}
	}
}

export class SyncKVServer implements SyncKVServerApi {
	private mutators: Record<string, AnyMutator>

	private db: Patch[]
	private version: number
	private lastMutationIds: Map<string, string> = new Map()
	private clients: SyncKVClientApi[] = []

	constructor(
		mutators: Record<string, AnyMutator>,
		initialState?: Database,
		private logger?: TupleLoggerApi
	) {
		this.mutators = mutators
		this.db = initialState ? [initialState] : []
		this.version = this.db.length
	}

	async pull(clientId: string, cookie: Cookie = 0) {
		this.logger?.log(
			"Received pull, client id: " +
				clientId +
				", cookie: " +
				cookie +
				", db: " +
				JSON.stringify(this.db, undefined, 2)
		)
		// Find the last mutation id for this client
		const lastMutationId = this.lastMutationIds.get(clientId)
		if (lastMutationId) {
			// We're going to ack this mutation id, so we can forget it
			this.lastMutationIds.delete(clientId)
		}

		const patchesSinceCookie = this.db.slice(cookie)
		this.logger?.log(
			"patchesSinceCookie",
			JSON.stringify(patchesSinceCookie, null, 2)
		)

		const patch = patchesSinceCookie.reduce((acc, patch) => {
			return { ...acc, ...patch }
		}, {})

		this.logger?.log("Sending", JSON.stringify(patch, undefined, 2), {
			lastMutationId,
		})

		return {
			cookie: this.db.length,
			patch,
			lastMutationId,
		}
	}

	async push(clientId: string, mutations: Mutation[]) {
		this.logger?.log("Handling push", {
			clientId,
			mutations,
		})
		const tx = new ServerTransaction(this.db)
		for (const { key, args } of mutations) {
			this.mutators[key](tx, ...args)
		}
		this.db.push(tx.patch)
		this.logger?.log("New db", JSON.stringify(this.db, undefined, 2))
		this.version++

		// Record last mutation id for this client
		const lastMutationId = mutations[mutations.length - 1].mutationId
		this.lastMutationIds.set(clientId, lastMutationId)

		this.pokeAllClients()
	}

	connectToClient(client: SyncKVClientApi) {
		this.clients.push(client)

		return () => {
			const index = this.clients.indexOf(client)
			if (index === -1) return
			this.clients.splice(index, 1)
		}
	}

	pokeAllClients() {
		for (const client of this.clients) {
			client.poke()
		}
	}

	get(key: string) {
		for (let i = this.db.length - 1; i >= 0; i--) {
			const patch = this.db[i]
			if (key in patch) {
				return patch[key]
			}
		}

		return undefined
	}
}

class Transaction {
	private db: Readonly<Database>
	readonly mutationId = randomId()
	patch: Patch = {}

	constructor(db: Readonly<Database>) {
		this.db = db
	}

	get(key: string) {
		return this.patch[key] || this.db[key]
	}

	set(key: string, value: any) {
		this.patch[key] = value
	}
}

class ServerTransaction {
	private db: Readonly<Patch[]>
	patch: Patch = {}

	constructor(db: Readonly<Patch[]>) {
		this.db = db
	}

	get(key: string) {
		if (key in this.patch) {
			return this.patch[key]
		}

		for (let i = this.db.length - 1; i >= 0; i--) {
			const patch = this.db[i]
			if (key in patch) {
				return patch[key]
			}
		}
	}

	set(key: string, value: any) {
		this.patch[key] = value
	}
}

export function mutation(fn: AnyMutator) {
	return fn
}

function randomId() {
	return Math.random().toString(36).slice(2)
}

type TupleLoggerApi = {
	log: (...messages: any[]) => void
	subspace: (subspaceName: string) => TupleLoggerApi
}

export function tupleLogger(subspace: string[] = []): TupleLoggerApi {
	return {
		log(...message) {
			console.log(...subspace, ...message)
		},
		subspace(subspaceName) {
			return tupleLogger([...subspace, subspaceName])
		},
	}
}
