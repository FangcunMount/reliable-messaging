// Package redis provides lossy wake-up hints using a host-owned Redis client.
// Publish success does not acknowledge a subscriber or durable business result.
// Constructors perform no network IO. Watch owns only its subscription and the
// caller owns its context, execution loop, Redis client, and polling fallback.
// This adapter is not a reliable broker or an Outbox transport.
package redis
