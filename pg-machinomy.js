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
   *     signature_valid: true | false
   *     is_latest: true | false | null (if sig isn't valid)
   *     dupe_status:
   *       'distinct'  | // Not a duplicate
   *       'duplicate' | // An exact duplicate state exists
   *       'conflict'  | // A state with the same serial number but different exists
   *   }
   */
  getStateUpdateStatus(stateUpdate) {
    return this._queryOne('res', SQL`
      SELECT mcy_state_update_status(${stateUpdate}::jsonb) AS res
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
   *     channel_payment: eth (identical to latest_state.amount)
   *     // If a channel exists and has a value, `channel.value - latest_state.amount`.
   *     channel_remaining_balance: null | eth
   *   }
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
   * Inserts a Channel, setting the state to `CS_PENDING`.
   *
   * Returns:
   *
   *   {
   *     created: true | false (if channel with the same chain, contract, and channel exists)
   *     channel: Channel
   *   }
   *
   * Where:
   *
   *   Channel = {
   *     id: integer,
   *     chain_id: integer,
   *     contract_id: eth_address,
   *     channel_id: eth_address,
   *
   *     sender: eth_address,
   *     receiver: eth_address,
   *     value: null | eth,
   *     settlement_period: null | integer,
   *     payment: null | eth,
   *
   *     status: 'CS_PENDING' | 'CS_OPEN' | 'CS_SETTLING' | 'CS_SETTLED'
   *     opened_on: null | timestamp
   *     settlement_started_on: null | timestamp,
   *     settlement_finalized_on: null | timestamp,
   *  }
   */
  insertChannel(channel) {
    return this._queryOne('res', SQL`
      SELECT mcy_insert_channel(${channel}) AS res
    `);
  }

  /**
   * Gets a channel's state.
   *
   * Returns:
   *
   *   {
   *     channel: Channel,
   *     latest_state: StateUpdate,
   *     // The payment and remaining balance will be set and non-null if
   *     // the channel a value and there's a latest_state.
   *     current_payment: null | eth,
   *     current_remaining_balance: null | eth,
   *   }
   */
  getChannelState(channel) {
    return this._queryOne('res', SQL`
      SELECT mcy_get_channel_state(${channel}) AS res
    `);
  }

  /**
   * Inserts a ChannelEvent and updates the channel's state (value, status,
   * etc) accordingly.
   *
   * Check that it's sensible to transition the event's channel into the new
   * state (ex, it's sensible to transition from CS_OPEN to CS_PENDING, but
   * it isn't sensbile to transition from CS_PENDING to CS_OPEN), but does
   * _not_ check whether the event is consistent (ex, does not check whether
   * a payment is greater than the channel's value).
   *
   * On success, returns: Channel
   *
   * On error, inserts the invalid event into the invalid_channel_events table
   * and returns:
   *
   *   {
   *     error: true,
   *     reason: 'developer friendly error message',
   *     type: 'invalid_channel_status' | 'invalid_event_type',
   *     
   *     when type = 'invalid_channel_status':
   *       expected_statuses: [ChannelStatus, ...],
   *       actaul_status: ChannelStatus
   *
   *     when type = 'invalid_event_type':
   *       expected_types: [EventType, ...],
   *       actual_type: EventType,
   *   }
   */
  insertChannelEvent(event) {
    return this._queryOne('res', SQL`
      SELECT mcy_insert_channel_event(${event}) AS res
    `);
  }

}

module.exports = {
  PGMachinomy: PGMachinomy,
};
