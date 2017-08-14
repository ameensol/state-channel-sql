let fs = require('fs');

let _pga = require('pg-async');
let [PgAsync, SQL] = [_pga['default'], _pga.SQL];

class PGMachinomy {
  /**
   * Where pgUrl can either be a connection string:
   *
   *   postgres://user:password@host/database
   *
   * Or a connection object:
   *
   *   { user, password, host, port, database, ... }
   */
  constructor(pgUrl) {
    this.pg = new PgAsync(pgUrl);
  }

  _query(...args) {
    return this.pg
      .query(...args)
      .then(res => res.rows);
  }

  _queryOne(field, query) {
    if (!query) {
      query = field;
      field = null;
    }
    return this.pg
      .query(query)
      .then(res => field? res.rows[0][field] : res.rows[0]);
  }

  /**
   * Loads tables and functions into the database. Should only be called once.
   */
  setupDatabase() {
    let sql = fs.readFileSync(__dirname + '/pg-machinomy.sql', 'utf-8');
    return this._query(sql);
  }

  /**
   * Closes the database connection.
   */
  close() {
    this.pg.closeConnections();
  }

  /**
   * Performs a simple query to make sure the connection works.
   */
  selftest() {
    return this._queryOne(`
      SELECT current_setting('server_version_num') AS pg_version
    `);
  }

  /**
   * Gets the status of a StateUpdate. Without inserting the state update,
   * returns:
   *
   *   StateUpdateStatus = {
   *     signature_valid: true | false,
   *     is_latest: true | false,
   *     dupe_status:
   *       'distinct'  | // Not a duplicate
   *       'duplicate' | // An exact duplicate state exists
   *       'conflict'  | // A state with the same serial number but different exists
   *     // The amount of wei this update adds to the channel. Will be ``null``
   *     // if this update isn't the latest.
   *     added_amount: wei_amount | null,
   *   }
   */
  getStateUpdateStatus(stateUpdate) {
    return this._queryOne('res', SQL`
      SELECT mcy_get_state_update_status(${stateUpdate}::jsonb) AS res
    `);
  }

  /**
   * Inserts a StateUpdate.
   *
   * On success, returns:
   *
   *   {
   *     // If the state was inserted, the ID of the new row in the
   *     // state_updates table. Nothing will be inserted if the new state is
   *     // an exact duplicate of an existing state
   *     id: null | integer
   *     // If the new state was inserted (see above)
   *     created: true | false
   *     status: StateUpdateStatus
   *     is_latest: true | false
   *     latest_state: StateUpdate of the latest state in the channel
   *     channel_payment: wei_amount (identical to latest_state.amount)
   *     // If a channel exists and has a value, `channel.value - latest_state.amount`.
   *     channel_remaining_balance: null | wei_amount
   *     // The amount of wei this update adds to the channel. See
   *     // StateUpdateStatus.added_amount.
   *     added_amount: wei_amount | null
   *   }
   *
   * Where:
   *
   *   StateUpdate = {
   *     chain_id: int,
   *     contract_id: eth_address,
   *     channel_id: sha3_hash,
   *     ts: Date,
   *
   *     amount: wei_amount,
   *     signature: secp256k1_sig,
   *  }
   *
   * On error, inserts the StateUpdate into the invalid_state_updates table
   * and returns:
   *
   *   {
   *     error: true,
   *     status: StateUpdateStatus
   *     reason: 'signature_invalid' | 'conflict' | 'invalid_state: <developer friendly reason>'
   *   }
   */
  insertStateUpdate(stateUpdate) {
    return this._queryOne('res', SQL`
      SELECT mcy_insert_state_update(${stateUpdate}::jsonb) AS res
    `);
  }

  /**
   * Gets the latest state for a Channel.
   *
   * Returns: StateUpdate.
   */
  getLatestState(channel) {
    return this._queryOne('res', SQL`
      SELECT mcy_state_update_row_to_json(mcy_get_latest_state_update(${channel})) AS res
    `);
  }

  /**
   * Gets the status of a channel, including the latest state and latest events.
   *
   * If `includeIntent` is set (the default), the result will include intent
   * events and will reflect the intended state of the channel. Otherwise, only
   * blockchain events will be used to determine the channel's stauts (ie, the
   * result will be the channel as it exists on the blockchain). See also: the
   * `channel.state_is_intent` field.
   *
   * If there have been conflicting events (for example, a `DidDeposit` event
   * before a `DidCreateChannel` event), the `is_invalid` field will be set
   * to `true`, and `is_invalid_reason` will be a developer-friendly
   * description of the problem:
   *
   *   "invalid channel state for event DidDeposit: got NULL but should be CS_OPEN"
   *
   * If `is_invalid` is set, the status returned will be that of the channel
   * *before* the invalid event.
   *
   * Note: it's possible for reorgs to put a channel into an inconsistent state
   * without `is_invalid` being set. Consider, for example, a sitaution where
   * two blocks contain a `DidDeposit` event, then one of those blocks is
   * orphaned; the channel will have an incorrect value, but there's no way to
   * detect this situation (until `setRecentBlocks` is called and the orphaned
   * block is marked invalid).
   *
   * Returns:
   *
   *   ChannelStatus = {
   *     channel: Channel
   *
   *     // QUESTION: Are these useful?
   *     latest_event: null | ChannelEvent | IntentEvent,
   *     latest_intent_event: null | IntentEvent,
   *     latest_chain_event: ChannelEvent
   *
   *     latest_state: null | StateUpdate,
   *
   *     // `current_payment` and `current_remaining_balance` are
   *     // shortcuts for `current_payment = latest_state.amount` and
   *     // `channel.value - current_payment`.
   *     current_payment: null | wei_amount,
   *     current_remaining_balance: null | wei_amount,
   *
   *     is_invalid: boolean,
   *     is_invalid_reason: null | developer-friendly reason
   *   }
   *
   * Where:
   *
   *   Channel = {
   *     chain_id: int,
   *     contract_id: eth_address,
   *     channel_id: sha3_hash,
   *
   *     sender: null | eth_address,
   *     receiver: null | eth_address,
   *     value: null | wei_amount,
   *     settlement_period: null | int,
   *     until: null | timestamp,
   *
   *     payment: null | wei_amount,
   *     odd_value: null | wei_amount,
   *
   *     // If any of the events related to this channel are intent events,
   *     // state_is_intent will be true, meaning the state represented here isn't
   *     // yet confirmed on the blockchain.
   *     state_is_intent: null | boolean,
   *     state: null | 'CS_OPEN' | 'CS_SETTLING' | 'CS_SETTLED',
   *
   *     opened_on: null | timestamp,
   *     settlement_started_on: null | timestamp,
   *     settlement_finalized_on: null | timestamp
   *   }
   *
   */
  getChannelStatus(channel, includeIntent=true) {
    return this._queryOne('res', SQL`
      SELECT mcy_get_channel_status(${channel}, ${includeIntent}) AS res
    `);
  }

  /**
   * Inserts a ChannelEvent and returns the new channel.
   *
   *   ChannelEvent = {
   *     chain_id: int,
   *     contract_id: eth_address,
   *     channel_id: sha3_hash,
   *     ts: timestamp,
   *
   *     block_number: int,
   *     block_hash: sha3_hash,
   *     // `block_is_valid` will be `false` if `setRecentBlocks` has rendered
   *     // this event's block invalid (orphaned).
   *     // This field is ingnored when inserting an event.
   *     block_is_valid: boolean,
   *
   *     sender: eth_address,
   *     event_type: 'DidCreateChannel' | 'DidDeposit' | 'DidStartSettle' | 'DidSettle'
   *     fields: { event-specifc fields (ex, `value` when `event_type` is 'DidDeposit') }
   *  }
   */
  insertChannelEvent(event) {
    return this._queryOne('res', SQL`
      SELECT mcy_insert_channel_event(${event}) AS res
    `);
  }

  /**
   * Inserts an IntentEvent. IntentEvents have a structure identical to
   * ChannelEvents, but are used to signify that the caller *intends* to
   * perform some action so the database can correctly reflect the state
   * we expect once the real event gets to the blockchain.
   *
   *   IntentEvents = {
   *     chain_id: int,
   *     contract_id: eth_address,
   *     channel_id: sha3_hash,
   *     ts: timestamp,
   *
   *     // The last block number seen at the time this event was created. Used
   *     // so intent events can be correctly ordered along side
   *     // blockchain-based ChannelEvent.
   *     block_number: int,
   *
   *     sender: eth_address,
   *     event_type: 'DidCreateChannel' | 'DidDeposit' | 'DidStartSettle' | 'DidSettle'
   *     fields: { event-specifc fields (ex, `value` when `event_type` is 'DidDeposit') }
   *  }
   */
  insertChannelIntent(intent) {
    return this._queryOne('res', SQL`
      SELECT mcy_insert_channel_intent(${intent}) AS res
    `);
  }

  /**
   * Handle chain reorgs by setting the most recently seen blocks. Events which
   * are in blocks that are now orphaned will be removed, and events which may
   * have been in previously orphaned blocks are restored.
   *
   * The `firstBlockNum` should be the number of the first block in
   * `blockHashes`.
   *
   * Returns a list of the channels which have changed in light of this update
   * (see the result of `getChannelStatus`).
   *
   * For example:
   *
   *   > setRecentBlocks(1, 100, ["abc123", "fff456", ...])
   *   {
   *     // The number of events that were updated (nb, many events in one
   *     // channel may have been updated).
   *     updated_event_count: 3,
   *     updated_channels: [ ChannelStatus, ... ],
   *   }
   *
   */
  setRecentBlocks(chainId, firstBlockNum, blockHashes) {
    return this._queryOne('res', SQL`
      SELECT mcy_set_recent_blocks(${chainId}, ${firstBlockNum}, ${blockHashes}) as res;
    `);
  }

  /**
   * Returns all of the events for a particular channel in order, oldest to
   * newest.
   *
   * If `includeIntent` is `true` (default), intent events will be included.
   *
   * Returns:
   *
   *   [ ChannelEvent | IntentEvent, ... ]
   */
  getChannelEvents(chan, includeIntent=true) {
    return this._query(SQL`
      SELECT * FROM mcy_get_channel_events(${chan}, ${includeIntent});
    `);
  }

}

async function monkeypatchSignatureVerification(pgm) {
  // Grab the original functions
  let realVerifyFuncs = await pgm._queryOne('res', SQL`
    select
      proname,
      array_agg(pg_get_functiondef(pp.oid)) as res
    from pg_proc pp
    inner join pg_namespace pn on (pp.pronamespace = pn.oid)
    inner join pg_language pl on (pp.prolang = pl.oid)
    where
      proname = 'ecdsa_verify'
    group by proname
  `);

  // Replace each original function with a stub that always reutrns true
  await Promise.all(realVerifyFuncs.map(async (func) => {
    let sig = func.toLowerCase().match(/ecdsa_verify\(.*\)/)[0];
    await pgm._query(`
      ${func.replace('public.ecdsa_verify(', 'real_ecdsa_verify(')};
    `);

    await pgm._query(`
      create or replace function ${sig} returns boolean
      language sql as $$ select real_ecdsa_verify($1, $2, $3, $4, $5) or true $$;
    `);
  }));

  return realVerifyFuncs;
}


module.exports = {
  PGMachinomy: PGMachinomy,
  monkeypatchSignatureVerification: monkeypatchSignatureVerification,
};
