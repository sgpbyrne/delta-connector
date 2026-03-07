package com.deltaconnect.protocol.actions

/**
 * Sealed interface representing all Delta Lake transaction log action types.
 *
 * Each action corresponds to a single JSON line in a `_delta_log/NNNNN.json` commit file,
 * wrapped in a type discriminator key: `{"add": {...}}`, `{"remove": {...}}`, etc.
 *
 * See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
 */
sealed interface Action
