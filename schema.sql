CREATE TABLE channel_updates (
  id INTEGER PRIMARY KEY,
  chain_id INTEGER,
  contract_id BYTEA,
  channel_id BYTEA,
  amount INTEGER,
  sequence_number INTEGER,
  sig BYTEA,
  time TIMESTAMPTZ
);

CREATE TABLE channels (
  chain_id INTEGER,
  contract_id BYTEA,
  channel_id BYTEA,
  sender BYTEA,
  receiver BYTEA,
  token_address BYTEA,
  deposit INTEGER,
  balance INTEGER,
  created_block INTEGER,
  settlement_period INTEGER,
  settlement_expiration INTEGER,
  state INTEGER, /* enum ChannelState { Open, Settling, Settled } */
  last_update INTEGER REFERENCES channel_updates (id)
  PRIMARY KEY (chain_id, contract_id, channel_id)
);
