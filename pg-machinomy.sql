--
-- Data types
--

create domain mcy_eth_address as varchar(40)
check ( length(value) = 40 );

create domain mcy_sha3_hash as varchar(64)
check ( length(value) = 64 );

-- hopefully we'll never need to store more than 1e(1000-18) either...
create domain mcy_eth as numeric(1000, 18);

create domain mcy_secp256k1_sig as varchar(128)
check ( length(value) = 128 );


--
-- State updates table
--

create table state_updates (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,
    ts timestamp not null,

    sequence_num int not null,
    amount mcy_eth not null,
    signature mcy_secp256k1_sig not null
);

create unique index state_updates_chain_contract_chan_seq_unique_idx
on state_updates (chain_id, contract_id, channel_id, sequence_num);


create table invalid_state_updates (
    id bigserial primary key,
    inserted_on timestamp not null,

    reason text not null,
    status jsonb not null,

    blob jsonb not null
);


--
-- Channels
--

create type mcy_channel_status as enum (
    'CS_PENDING',  -- Pending blockchain confirmation
    'CS_OPEN',     -- Open for business
    'CS_SETTLING', -- Sender has requested settlement
    'CS_SETTLED'   -- Channel has been settled
);

create table channels (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,

    sender mcy_eth_address not null,
    receiver mcy_eth_address not null,
    value mcy_eth,
    settlement_period integer,
    payment mcy_eth,

    status mcy_channel_status not null,
    opened_on timestamp null,
    settlement_started_on timestamp null,
    settlement_finalized_on timestamp null
);

create unique index channels_chain_contract_channel_unique_idx
on channels (chain_id, contract_id, channel_id);


--
-- Events
--

create type mcy_channel_event_type as enum (
    -- As defined in Machinomy's Broker.sol
    'DidCreateChannel',
    'DidDeposit',
    'DidStartSettle',
    'DidSettle'
);


create table channel_events (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,
    ts timestamp not null,

    block mcy_sha3_hash not null,
    sender mcy_eth_address not null,
    event_type mcy_channel_event_type not null,

    fields jsonb not null
);

create table invalid_channel_events (
    id bigserial primary key,
    inserted_on timestamp not null,
    type text not null,
    reason text not null,
    blob jsonb not null
);

create function mcy_insert_channel_event(event jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    channel channels;
    event_type text := (event->>'type');
    fields jsonb := (event->'fields');
    error jsonb;
begin
    select *
    from channels
    where
        chain_id = (event->>'chain_id')::int and
        contract_id = (event->>'contract_id')::mcy_eth_address and
        channel_id = (event->>'channel_id')::mcy_sha3_hash
    into channel
    for no key update;

    -- DidCreateChannel (when a channel does not exist)
    if channel.id is null then
        error := mcy_assert_channel_event(event, 'DidCreateChannel');
        if error is not null then
            return error;
        end if;

        begin
            insert into channels (
                chain_id, contract_id, channel_id,
                sender, receiver, status, opened_on
            )
            values (
                (event->>'chain_id')::int,
                (event->>'contract_id')::mcy_eth_address,
                (event->>'channel_id')::mcy_sha3_hash,
                (fields->>'sender')::mcy_eth_address,
                (fields->>'receiver')::mcy_eth_address,
                'CS_OPEN',
                to_timestamp((event->>'ts')::int)
            )
            returning * into channel;
        exception when unique_violation then
            return mcy_insert_channel_event(event);
        end;

    -- DidCreateChannel (when a channel already exists)
    elsif channel.status = 'CS_PENDING' then
        error := mcy_assert_channel_event(event, 'DidCreateChannel');
        if error is not null then
            return error;
        end if;

        update channels
        set
            sender = (fields->>'sender')::mcy_eth_address,
            receiver = (fields->>'receiver')::mcy_eth_address,
            status = 'CS_OPEN',
            opened_on = to_timestamp((event->>'ts')::int)
        where id = channel.id
        returning * into channel;

    -- DidDeposit
    elsif event_type = 'DidDeposit' then
        error := mcy_assert_channel_status(channel, event, 'CS_OPEN');
        if error is not null then
            return error;
        end if;

        update channels
        set
            value = (fields->>'value')::mcy_eth
        where id = channel.id
        returning * into channel;

    -- DidStartSettle
    elsif event_type = 'DidStartSettle' then
        error := mcy_assert_channel_status(channel, event, 'CS_OPEN');
        if error is not null then
            return error;
        end if;

        update channels
        set
            settlement_started_on = to_timestamp((event->>'ts')::int),
            payment = (fields->>'payment')::mcy_eth,
            status = 'CS_SETTLING'
        where id = channel.id
        returning * into channel;

    -- DidSettle
    elsif event_type = 'DidSettle' then
        error := mcy_assert_channel_status(channel, event, 'CS_OPEN', 'CS_SETTLING');
        if error is not null then
            return error;
        end if;

        update channels
        set
            settlement_finalized_on = to_timestamp((event->>'ts')::int),
            payment = (fields->>'payment')::mcy_eth,
            status = 'CS_SETTLED'
        where id = channel.id
        returning * into channel;

    else
        error := mcy_assert_channel_event(event, 'SOME_VALID_TYPE');
        return error;
    end if;

    insert into channel_events (
        chain_id, contract_id, channel_id,
        ts, block, sender, event_type, fields
    )
    values (
        (event->>'chain_id')::int,
        (event->>'contract_id')::mcy_eth_address,
        (event->>'channel_id')::mcy_sha3_hash,
        to_timestamp((event->>'ts')::int),
        (event->>'block')::mcy_sha3_hash,
        (event->>'sender')::mcy_eth_address,
        (event->>'type')::mcy_channel_event_type,
        fields
    );

    return row_to_json(channel);
end
$pgsql$;


-- Asserts that `event`'s type is `expected_type`. If it isn't, `event` is
-- inserted into `invalid_channel_events` and an error is returned.
create function mcy_assert_channel_event(event jsonb, expected_type mcy_channel_event_type)
returns jsonb
language plpgsql as $pgsql$
declare
    event_actual_type mcy_channel_event_type := (event->>'type');
    err_msg text;
begin
    if event_actual_type = expected_type then
        return null;
    end if;
    err_msg := (
        'Invalid event for channel state: ' || event_actual_type ||
        ' (expected: ' || expected_type || ')'
    );

    insert into invalid_channel_events (inserted_on, type, reason, blob)
    values (now(), 'invalid_event_type', err_msg, event);

    return json_build_object(
        'error', true,
        'reason', err_msg,
        'type', 'invalid_event_type',
        'expected_types', array[expected_type],
        'actual_type', event_actual_type
    );
end
$pgsql$;


-- Asserts that `channel`'s status is `expected_a` or `expected_b`. If it
-- isn't, `event` is inserted into `invalid_channel_events` and an error is
-- returned.
create function mcy_assert_channel_status(
    channel channels, event jsonb,
    expected_a mcy_channel_status, expected_b mcy_channel_status default null
)
returns jsonb
language plpgsql as $pgsql$
declare
    err_msg text;
    b_msg text;
begin
    if (channel.status = expected_a or channel.status = expected_b) then
        return null;
    end if;
    b_msg := (
        case when expected_b is null
        then ''
        else (' or ' || expected_b)
        end
    );
    err_msg := (
        'Invalid status for channel: ' || channel.status ||
        ' (expected: ' || expected_a || b_msg || ')'
    );

    insert into invalid_channel_events (inserted_on, type, reason, blob)
    values (now(), 'invalid_channel_status', err_msg, event);

    return json_build_object(
        'error', true,
        'reason', err_msg,
        'type', 'invalid_channel_status',
        'expected_statuses', (
            case when expected_b is null
            then array[expected_a]
            else array[expected_a, expected_b]
            end
        ),
        'actual_status', channel.status
    );
end
$pgsql$;


create function mcy_insert_channel(chan jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    channel channels;
begin
    insert into channels (
        chain_id, contract_id, channel_id,
        sender, receiver, status
    )
    values (
        (chan->>'chain_id')::int,
        (chan->>'contract_id')::mcy_eth_address,
        (chan->>'channel_id')::mcy_sha3_hash,
        (chan->>'sender')::mcy_eth_address,
        (chan->>'receiver')::mcy_eth_address,
        'CS_PENDING'
    )
    returning * into channel;
    return json_build_object(
        'created', true,
        'channel', row_to_json(channel)
    );
exception when unique_violation then
    select *
    from channels
    where
        chain_id = (chan->>'chain_id')::int and
        contract_id = (chan->>'contract_id')::mcy_eth_address and
        channel_id = (chan->>'channel_id')::mcy_sha3_hash
    into channel;
    return json_build_object(
        'created', false,
        'channel', row_to_json(channel)
    );
end
$pgsql$;


create function mcy_get_channel_state(chan jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    channel channels;
    latest_state state_updates;
begin
    latest_state := mcy_get_latest_state_update(chan);

    select *
    from channels
    where
        chain_id = (chan->>'chain_id')::int and
        contract_id = (chan->>'contract_id')::mcy_eth_address and
        channel_id = (chan->>'channel_id')::mcy_sha3_hash
    into channel;

    return json_build_object(
        'channel', row_to_json(channel),
        'latest_state', latest_state,
        'current_payment', latest_state.amount,
        'current_remaining_balance', channel.value - latest_state.amount
    );
end
$pgsql$;


--
-- Functions
--

create function mcy_state_update_status(state_update jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    is_valid boolean := mcy_state_update_is_signature_valid(state_update);
begin
    -- TODO: actaully check the state
    -- signature_valid: true | false
    -- is_latest: true | false | null (if sig isn't valid)
    -- dupe_status: 'distinct' | 'duplicate' | 'conflict' | null
    return ('{
        "signature_valid": ' || is_valid || ',
        "is_latest": true,
        "dupe_status": "distinct"
    }')::jsonb;
end
$pgsql$;

create function mcy_state_update_is_signature_valid(state_update jsonb)
returns boolean
language sql as $$
    -- TODO: Actaully figure out how to check this signature
    select true::boolean as signature_valid
$$;

create function mcy_state_update_row_to_json(state_update state_updates)
returns jsonb
language sql as $$
    select row_to_json(state_update)::jsonb;
$$;

create function mcy_insert_state_update(state_update jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    status jsonb;

    new_row state_updates;
    latest_state state_updates;
    remaining_balance mcy_eth;
begin
    -- TODO: make sure we're doing the appropriate locking here so that
    -- state updates are serialized.
    status := mcy_state_update_status(state_update);

    if not (status->>'signature_valid')::boolean then
        return mcy_insert_invalid_state_update(
            'signature_invalid', status, state_update
        );
    end if;

    if (status->>'dupe_status') = 'conflict' then
        return mcy_insert_invalid_state_update(
            'conflict', status, state_update
        );
    end if;

    if (status->>'dupe_status' = 'distinct') then
        declare
            err_msg text;
        begin
            insert into state_updates (
                chain_id, contract_id, channel_id, ts, sequence_num, amount, signature
            )
            select
                (state_update->>'chain_id')::int as chain_id,
                (state_update->>'contract_id')::mcy_eth_address as contract_id,
                (state_update->>'channel_id')::mcy_sha3_hash as channel_id,
                to_timestamp((state_update->>'ts')::int) as ts,
                (state_update->>'sequence_num')::int as sequence_num,
                (state_update->>'amount')::mcy_eth as amount,
                (state_update->>'signature')::mcy_secp256k1_sig as signature
            returning * into new_row;
        exception when others then
            get stacked diagnostics err_msg = MESSAGE_TEXT;
            return mcy_insert_invalid_state_update(
                'invalid_state: ' || err_msg, status, state_update
            );
        end;
    end if;

    latest_state := (
        case when (status->>'is_latest')::boolean and new_row is not null
        then new_row
        else mcy_get_latest_state_update(state_update)
        end
    );

    remaining_balance := (
        select channel.value - latest_state.amount
        from channels as channel
        where
            chain_id = (state_update->>'chain_id')::int and
            contract_id = (state_update->>'contract_id')::mcy_eth_address and
            channel_id = (state_update->>'channel_id')::mcy_sha3_hash
    );

    return json_build_object(
        'id', new_row.id,
        'created', (new_row.id is not null),
        'status', status,
        'is_latest', status->'is_latest',
        'latest_state', (mcy_state_update_row_to_json(latest_state)),
        'channel_payment', latest_state.amount,
        'channel_remaining_balance', remaining_balance
    );
end
$pgsql$;


create function mcy_insert_invalid_state_update(reason text, status jsonb, blob jsonb)
returns jsonb
language plpgsql as $pgsql$
begin
    insert into invalid_state_updates (inserted_on, reason, status, blob)
    values (now(), reason, status, blob);

    return json_build_object(
        'error', true,
        'status', status,
        'reason', reason
    );
end
$pgsql$;


create function mcy_get_latest_state_update(chan jsonb)
returns state_updates
called on null input
language plpgsql as $pgsql$
declare
    chain_id_ int := (chan->>'chain_id')::int;
    contract_id_ mcy_eth_address := (chan->>'contract_id')::mcy_eth_address;
    channel_id_ mcy_sha3_hash := (chan->>'channel_id')::mcy_sha3_hash;
    latest_state state_updates;
begin
    if chain_id_ is null then
        raise exception 'chain_id must not be null' using errcode = 'null_value_not_allowed';
    end if;

    if contract_id_ is null then
        raise exception 'contract_id must not be null' using errcode = 'null_value_not_allowed';
    end if;

    if channel_id_ is null then
        raise exception 'channel_id must not be null' using errcode = 'null_value_not_allowed';
    end if;

    select sup.*
    from state_updates as sup
    where
        sup.chain_id = chain_id_ and
        sup.contract_id = contract_id_ and
        sup.channel_id = channel_id_
    order by sequence_num desc
    limit 1
    into latest_state;

    return latest_state;
end
$pgsql$;
