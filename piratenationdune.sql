-- https://dune.com/embeds/1789814/2948924   
-- Number of Holders, Single Holders, Most owned

WITH agg AS (

SELECT
wallet,
SUM(value) AS tokens

FROM (

-- ethereum

SELECT
"to" AS wallet,
1 AS value,
'minted' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'transfer' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000, 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND "to" NOT IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0


UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'bridge back to eth' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'bridge to polygon' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "to" IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0


UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'transfer' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000, 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND "to" NOT IN (0x0000000000000000000000000000000000000000, 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'burn' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "to" IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0


-- polygon
UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'minted' AS action
FROM erc721_polygon.evt_Transfer
WHERE "from" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943

UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'transfer' AS action
FROM erc721_polygon.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000)
AND "to" NOT IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943


UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'transfer' AS action
FROM erc721_polygon.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000)
AND "to" NOT IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943

UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'burn' AS action
FROM erc721_polygon.evt_Transfer
WHERE "to" IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943
)

GROUP BY 1
HAVING SUM(value) >0
ORDER BY 2 DESC
)


SELECT
COUNT(distinct wallet) AS owners,
MAX(tokens) AS most_owned,
AVG(tokens) AS average_owned,
COUNT(distinct wallet) filter (WHERE tokens = 1) AS single_token_wallets,
CAST(approx_percentile(tokens, 0.5) AS double) AS median,
CAST(approx_percentile(tokens, 0.25) AS double) AS "25th",
CAST(approx_percentile(tokens, 0.75) AS double) AS "75th"

-- percentile_cont(0.5) WITHIN group ( ORDER BY tokens) AS median,
-- percentile_cont(0.25) WITHIN group ( ORDER BY tokens) AS "25th",
-- percentile_cont(0.75) WITHIN group ( ORDER BY tokens) AS "75th"

FROM agg

WHERE wallet NOT IN (0x69eabd271ce10c2102498d7a0b00b6e113dcdfe8)



-- https://dune.com/queries/2058944/3406174
-- Number of ships

SELECT
SUM(ships) AS ships

FROM

(

SELECT
SUM(amount) AS ships
FROM nft.transfers
WHERE blockchain = 'polygon'
AND contract_address = '0xd29cb4237ddc383e4b5d5d9d106dc16f59256e91'
AND token_standard = 'erc721'
AND `from` = '0x0000000000000000000000000000000000000000'

UNION ALL

SELECT
-SUM(amount) AS ships
FROM nft.transfers
WHERE blockchain = 'polygon'
AND contract_address = '0xd29cb4237ddc383e4b5d5d9d106dc16f59256e91'
AND token_standard = 'erc721'
AND `to` = '0x0000000000000000000000000000000000000000'

)

-- https://dune.com/queries/1790369/2950610
-- Number of Players

-- version 2: captures all wallets involved, otherwise it would have been fee payers only

WITH all_days AS (

    WITH
days AS
(
    with days_seq as (
        SELECT
        sequence(
            (CAST('2022-12-09' AS timestamp))
            , date_trunc('day', cast(now() as timestamp))
            , interval '1' day) as day
    )

    SELECT
        days.day AS day
    FROM days_seq
    CROSS JOIN unnest(day) as days(day) --this is just doing explode like in spark sql
)

select * FROM days
    ),

traces AS (
SELECT
block_time,
tx_hash
FROM polygon.traces
WHERE
"from" IN
(
0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0,
0xa1E6c5f3cA1BB3f7ca7dd6d7843945Ece0F5df8A,
0x252181197f84b8A084F167c33040E333628F5145,
0x57a9b65cEaC6c5ee0CEC8Cd4571a7c3805B558D8,
0x687e4d88c45EbA1aefB0Ace6D127D038e7235943,
0x7B5e4b497556a0fB6B21D5Da20730b18745a7205,
0xa2fD4D61db9d374BD086c1A0dA8bCD1C7c160f1d,
0xB218186291e84159d35DC990Bf71B62612308A11,
0x468147C73eCDF91d9814dEDa433a211578b2D23A,
0x7E3Bbf706798a98e96d949B4d1759fB8F6858D77,
0x54684603D6ba9a0b196C5E4aD5e8F88F3A9Abba1,
0x969595ae87e981640847C82aD94d0D93c6a9484D,
0xa6a63412D2d8737b3C6Bd84f087c39d21A488670,
0x7a14481DbFCA148FF0985D02c7569c80362C801D,
0xd4DFF6412069ba2Cee4A1D4Ee9cAF36cB18584AD,
0x7A74D3b3F70569e09FAad9de8A96c4854E2FA568,
0x49F5695431137690b75FcFc8a589F9066d7bB350,
0x88C4767667f03baF7679Ae9246167df1fB3cBA70,
0xA6e7c5C4877C6384a00de0FC9f044E7251df5671,
0x169ed290954d0Aa529727E5CDBaf2C6b27DC707d,
0xA6e7c5C4877C6384a00de0FC9f044E7251df5671,
0xC90696D9C44eCeb70352d43eFb53e9F9D8EEfdEd,
0x0c2453FA0E08f1a97879b1d95c12Db4B8D5C0449,
0xA3Abc4CF387D14dE7DF78893805ad81Cf97933CE,
0x037e2a85594DCF05d29936f18E60Da2f892FAdF0,
0x7a74d3b3f70569e09faad9de8a96c4854e2fa568
)
OR "to" IN
(
0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0,
0xa1E6c5f3cA1BB3f7ca7dd6d7843945Ece0F5df8A,
0x252181197f84b8A084F167c33040E333628F5145,
0x57a9b65cEaC6c5ee0CEC8Cd4571a7c3805B558D8,
0x687e4d88c45EbA1aefB0Ace6D127D038e7235943,
0x7B5e4b497556a0fB6B21D5Da20730b18745a7205,
0xa2fD4D61db9d374BD086c1A0dA8bCD1C7c160f1d,
0xB218186291e84159d35DC990Bf71B62612308A11,
0x468147C73eCDF91d9814dEDa433a211578b2D23A,
0x7E3Bbf706798a98e96d949B4d1759fB8F6858D77,
0x54684603D6ba9a0b196C5E4aD5e8F88F3A9Abba1,
0x969595ae87e981640847C82aD94d0D93c6a9484D,
0xa6a63412D2d8737b3C6Bd84f087c39d21A488670,
0x7a14481DbFCA148FF0985D02c7569c80362C801D,
0xd4DFF6412069ba2Cee4A1D4Ee9cAF36cB18584AD,
0x7A74D3b3F70569e09FAad9de8A96c4854E2FA568,
0x49F5695431137690b75FcFc8a589F9066d7bB350,
0x88C4767667f03baF7679Ae9246167df1fB3cBA70,
0xA6e7c5C4877C6384a00de0FC9f044E7251df5671,
0x169ed290954d0Aa529727E5CDBaf2C6b27DC707d,
0xA6e7c5C4877C6384a00de0FC9f044E7251df5671,
0xC90696D9C44eCeb70352d43eFb53e9F9D8EEfdEd,
0x0c2453FA0E08f1a97879b1d95c12Db4B8D5C0449,
0xA3Abc4CF387D14dE7DF78893805ad81Cf97933CE,
0x037e2a85594DCF05d29936f18E60Da2f892FAdF0,
0x7a74d3b3f70569e09faad9de8a96c4854e2fa568
)
AND block_time >= NOW() - interval '30' day
),

final_table AS (

SELECT
DATE_TRUNC('day', block_time) AS day,
COUNT(DISTINCT(wallet)) AS active_user,
COUNT(DISTINCT(evt_tx_hash)) AS transaction_count
FROM
(
SELECT
evt_block_time AS block_time,
evt_tx_hash,
"from" AS wallet
FROM erc20_polygon.evt_Transfer
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day

UNION ALL

SELECT
evt_block_time AS block_time,
evt_tx_hash,
"to" AS wallet
FROM erc20_polygon.evt_Transfer
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day

UNION ALL

SELECT
evt_block_time AS block_time,
evt_tx_hash,
"from" AS wallet
FROM erc1155_polygon.evt_TransferSingle
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day

UNION ALL

SELECT
evt_block_time AS block_time,
evt_tx_hash,
"to" AS wallet
FROM erc1155_polygon.evt_TransferSingle
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day

UNION ALL

SELECT
evt_block_time AS block_time,
evt_tx_hash,
"from" AS wallet
FROM erc1155_polygon.evt_TransferBatch
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day

UNION ALL

SELECT
evt_block_time AS block_time,
evt_tx_hash,
"to" AS wallet
FROM erc1155_polygon.evt_TransferBatch
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day

) p

GROUP BY 1

)

SELECT a.day,
active_user,
transaction_count
FROM all_days a
JOIN final_table f
ON a.day = f.day
WHERE 1<={{refresh}}
AND a.day >= NOW() - interval '30' day

-- https://dune.com/queries/2056718/3402660
-- New Users over time

WITH base_table AS (
    SELECT
    block_time,
    "from" AS seller,
    "to" AS buyer,
    contract_address
    FROM nft.transfers
    WHERE blockchain = 'polygon'
    AND block_time >= timestamp '2022-12-01'
    AND contract_address IN
    (
    0x57a9b65ceac6c5ee0cec8cd4571a7c3805b558d8,
    0x687e4d88c45eba1aefb0ace6d127d038e7235943,
    0x7a74d3b3f70569e09faad9de8a96c4854e2fa568
    )
)

, stacking as(
    SELECT
        block_time,
        buyer AS address
    FROM base_table
    WHERE buyer NOT IN (0x0000000000000000000000000000000000000000)
    UNION
    SELECT
        block_time,
        seller AS address
    FROM base_table
    WHERE seller NOT IN (0x0000000000000000000000000000000000000000)
)

, earliest_trade as (
    select address, min(block_time) as earliest_date
    from stacking
    group by 1
)

, earliest_date_count as(
    select
        date_trunc('day', earliest_date) as date
        , count(address) as new_users
    from earliest_trade
    group by 1
)

, all_address_count as(
    select
        date_trunc('day', block_time) as date,
        count(distinct address) as all_users
    from stacking
    group by 1
)

select
    all_address_count.date
    , case when new_users is null then 0 else new_users end as new_users_cleaned
    , all_users - case when new_users is null then 0 else new_users end as existing_users
    , all_users
from all_address_count
left join earliest_date_count
  on all_address_count.date = earliest_date_count.date
where all_address_count.date BETWEEN cast('2022-12-01' as timestamp) and NOW()
and 1 <= {{refresh}}

-- https://dune.com/queries/1789065/2947553
-- Pirates marooned on ETH

-- pirates marooned on ETH

WITH owned AS (
SELECT
tokenId,
SUM(value) AS value
FROM
(
SELECT
1 AS value,
tokenId
FROM erc721_ethereum.evt_Transfer
WHERE contract_address = 0x31fe9d95dde43cf9893b76160f63521a9e3d26b0
AND "to" = 0x69eabd271ce10c2102498d7a0b00b6e113dcdfe8

UNION ALL

SELECT
-1 AS value,
tokenId
FROM erc721_ethereum.evt_Transfer
WHERE contract_address = 0x31fe9d95dde43cf9893b76160f63521a9e3d26b0
AND "from" = 0x69eabd271ce10c2102498d7a0b00b6e113dcdfe8
)

GROUP BY 1
HAVING SUM(value) > 0
)

SELECT

time,
AVG(total_pirates_marooned) AS total_pirates_marooned

FROM

(
SELECT
DATE_TRUNC('day',evt_block_time) AS time,
SUM(value) OVER (ORDER BY DATE_TRUNC('day',evt_block_time)) AS total_pirates_marooned

FROM (

SELECT
evt_block_time,
tokenId,
1 AS value,
'minted' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
evt_block_time,
tokenId,
1 AS value,
'bridged back to eth' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" = 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
evt_block_time,
tokenId,
-1 AS value,
'burned' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "to" IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
evt_block_time,
tokenId,
-1 AS value,
'bridged to polygon' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "to" IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

)
WHERE tokenId NOT IN
(SELECT tokenId FROM owned)

)

GROUP BY 1

-- https://dune.com/queries/1790235/2949626
-- # of Holders and tokens

SELECT
wallet,
SUM(value) AS tokens

FROM (

-- ethereum

SELECT
"to" AS wallet,
1 AS value,
'minted' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'transfer' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000, 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND "to" NOT IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0


UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'bridge back to eth' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'bridge to polygon' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "to" IN (0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0


UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'transfer' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000, 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND "to" NOT IN (0x0000000000000000000000000000000000000000, 0xe6f45376f64e1f568bd1404c155e5ffd2f80f7ad)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0

UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'burn' AS action
FROM erc721_ethereum.evt_Transfer
WHERE "to" IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x31fe9d95ddE43cf9893b76160F63521a9e3D26B0


-- polygon
UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'minted' AS action
FROM erc721_polygon.evt_Transfer
WHERE "from" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943

UNION ALL

SELECT
"to" AS wallet,
1 AS value,
'transfer' AS action
FROM erc721_polygon.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000)
AND "to" NOT IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943


UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'transfer' AS action
FROM erc721_polygon.evt_Transfer
WHERE "from" NOT IN (0x0000000000000000000000000000000000000000)
AND "to" NOT IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943

UNION ALL

SELECT
"from" AS wallet,
-1 AS value,
'burn' AS action
FROM erc721_polygon.evt_Transfer
WHERE "to" IN (0x0000000000000000000000000000000000000000)
AND contract_address = 0x687e4d88c45EbA1aefB0Ace6D127D038e7235943
)
WHERE wallet NOT IN (0x69eabd271ce10c2102498d7a0b00b6e113dcdfe8)
AND 1 <= {{refresh}}
GROUP BY 1
HAVING SUM(value) >0
ORDER BY 2 DESC

-- https://dune.com/queries/1882004/3094835
-- Number of Pirates by Level

SELECT level,
COUNT (*) AS no_of_pirates
FROM (
SELECT
-- bytea2numeric_v2(topic2) AS tokenId,
bytearray_to_uint256(topic2) AS tokenId,
-- bytearray_to_uint256("data") AS level
CAST(MAX(bytearray_to_uint256("data")) AS double) AS level
-- CAST(MAX(bytea2numeric_v2("data")) AS double) AS level
FROM polygon.logs
WHERE
block_time >= CAST('2022-11-01' AS timestamp)
AND contract_address = 0x83191cc99b97ea4b4e73361f862c44c8f8d8492b
AND topic0 = 0xfdeeaf66101caa3b9381f67b2283c02a2ce9bb45fbe01599bc502ecf3cd65dcb
AND topic1 = 0x000000000000000000000000687e4d88c45eba1aefb0ace6d127d038e7235943
AND 1 <= {{refresh}}
-- AND tx_hash = 0x09f887eede7e7be2f00b128c343b66a3db007beea5b9da425ba83c9ee2770a20
GROUP BY 1
-- ORDER BY 2 DESC
)


GROUP BY 1
ORDER BY 1

-- https://dune.com/queries/1934948/3191916
-- PGLD spent in economy


WITH

traces AS (
SELECT
DATE_TRUNC('day', block_time) AS day,
tx_hash
FROM polygon.traces
WHERE
`from` IN
(
'0x31fe9d95dde43cf9893b76160f63521a9e3d26b0',
'0xa1e6c5f3ca1bb3f7ca7dd6d7843945ece0f5df8a',
'0x252181197f84b8a084f167c33040e333628f5145',
'0x57a9b65ceac6c5ee0cec8cd4571a7c3805b558d8',
'0x687e4d88c45eba1aefb0ace6d127d038e7235943',
'0x7b5e4b497556a0fb6b21d5da20730b18745a7205',
'0xa2fd4d61db9d374bd086c1a0da8bcd1c7c160f1d',
'0xb218186291e84159d35dc990bf71b62612308a11',
'0x468147c73ecdf91d9814deda433a211578b2d23a',
'0x7e3bbf706798a98e96d949b4d1759fb8f6858d77',
'0x54684603d6ba9a0b196c5e4ad5e8f88f3a9abba1',
'0x969595ae87e981640847c82ad94d0d93c6a9484d',
'0xa6a63412d2d8737b3c6bd84f087c39d21a488670',
'0x7a14481dbfca148ff0985d02c7569c80362c801d',
'0xd4dff6412069ba2cee4a1d4ee9caf36cb18584ad',
'0x7a74d3b3f70569e09faad9de8a96c4854e2fa568',
'0x49f5695431137690b75fcfc8a589f9066d7bb350',
'0x88c4767667f03baf7679ae9246167df1fb3cba70',
'0xa6e7c5c4877c6384a00de0fc9f044e7251df5671',
'0x169ed290954d0aa529727e5cdbaf2c6b27dc707d',
'0xa6e7c5c4877c6384a00de0fc9f044e7251df5671',
'0xc90696d9c44eceb70352d43efb53e9f9d8eefded',
'0x0c2453fa0e08f1a97879b1d95c12db4b8d5c0449',
'0xa3abc4cf387d14de7df78893805ad81cf97933ce',
'0x037e2a85594dcf05d29936f18e60da2f892fadf0',
'0x7a74d3b3f70569e09faad9de8a96c4854e2fa568'
)
OR `to` IN
(
'0x31fe9d95dde43cf9893b76160f63521a9e3d26b0',
'0xa1e6c5f3ca1bb3f7ca7dd6d7843945ece0f5df8a',
'0x252181197f84b8a084f167c33040e333628f5145',
'0x57a9b65ceac6c5ee0cec8cd4571a7c3805b558d8',
'0x687e4d88c45eba1aefb0ace6d127d038e7235943',
'0x7b5e4b497556a0fb6b21d5da20730b18745a7205',
'0xa2fd4d61db9d374bd086c1a0da8bcd1c7c160f1d',
'0xb218186291e84159d35dc990bf71b62612308a11',
'0x468147c73ecdf91d9814deda433a211578b2d23a',
'0x7e3bbf706798a98e96d949b4d1759fb8f6858d77',
'0x54684603d6ba9a0b196c5e4ad5e8f88f3a9abba1',
'0x969595ae87e981640847c82ad94d0d93c6a9484d',
'0xa6a63412d2d8737b3c6bd84f087c39d21a488670',
'0x7a14481dbfca148ff0985d02c7569c80362c801d',
'0xd4dff6412069ba2cee4a1d4ee9caf36cb18584ad',
'0x7a74d3b3f70569e09faad9de8a96c4854e2fa568',
'0x49f5695431137690b75fcfc8a589f9066d7bb350',
'0x88c4767667f03baf7679ae9246167df1fb3cba70',
'0xa6e7c5c4877c6384a00de0fc9f044e7251df5671',
'0x169ed290954d0aa529727e5cdbaf2c6b27dc707d',
'0xa6e7c5c4877c6384a00de0fc9f044e7251df5671',
'0xc90696d9c44eceb70352d43efb53e9f9d8eefded',
'0x0c2453fa0e08f1a97879b1d95c12db4b8d5c0449',
'0xa3abc4cf387d14de7df78893805ad81cf97933ce',
'0x037e2a85594dcf05d29936f18e60da2f892fadf0',
'0x7a74d3b3f70569e09faad9de8a96c4854e2fa568'
)
AND block_time >= CAST('2022-12-09' AS timestamp)
)




SELECT
DATE_TRUNC('day', evt_block_time) AS day,
SUM(CAST(value AS double)/1e18) AS gold_spent
-- evt_block_time AS block_time,
-- evt_tx_hash,
-- `from` AS wallet,
-- (value/1e18) AS gold_spent
FROM erc20_polygon.evt_Transfer
WHERE evt_tx_hash IN
(SELECT tx_hash
FROM traces)
AND evt_block_time >= NOW() - interval '30' day
AND contract_address = '0x252181197f84b8a084f167c33040e333628f5145'
AND `from` != '0x0000000000000000000000000000000000000000'
AND 1<= {{refresh}}

GROUP BY 1

-- https://dune.com/queries/1789259/2947872
-- PGLD Minted and Burned


WITH

all_days AS (

    with
days AS
(
    with days_seq as (
        SELECT
        sequence(
            (SELECT cast(min(date_trunc('day', evt_block_time)) as timestamp) day
            FROM
            (SELECT evt_block_time
            FROM erc20_polygon.evt_Transfer
            WHERE contract_address = 0x252181197f84b8A084F167c33040E333628F5145 )
            tr)
            , date_trunc('day', cast(now() as timestamp))
            , interval '1' day) as day
    )

    SELECT
        days.day AS day
    FROM days_seq
    CROSS JOIN unnest(day) as days(day) --this is just doing explode like in spark sql
)

select * FROM days
    ),


minted AS (
SELECT
DATE_TRUNC('day', evt_block_time) AS day,
SUM(CAST(value AS double)/1e18) AS pgld_minted
FROM erc20_polygon.evt_Transfer
WHERE
"from" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x252181197f84b8A084F167c33040E333628F5145
GROUP BY 1

),

burned AS (
SELECT
DATE_TRUNC('day', evt_block_time) AS day,
-SUM(CAST(value AS double)/1e18) AS pgld_burned
FROM erc20_polygon.evt_Transfer
WHERE
"to" = 0x0000000000000000000000000000000000000000
AND contract_address = 0x252181197f84b8A084F167c33040E333628F5145
AND 1 <= {{refresh}}
GROUP BY 1
)

SELECT a.day,
m.pgld_minted,
b.pgld_burned,
m.pgld_minted+b.pgld_burned AS net,
SUM((m.pgld_minted+b.pgld_burned)) OVER (ORDER BY a.day) AS circulating_supply
FROM all_days a
FULL JOIN minted m
ON a.day = m.day
FULL JOIN burned b
ON a.day = b.day
WHERE a.day >= NOW() - interval '30' day


-- https://dune.com/queries/1885884/3101960
-- Founders' Iron Chest

WITH chests_received AS (

SELECT
DATE_TRUNC('day', evt_block_time) AS time,
'receive chest' AS action_type,
SUM(CAST(value AS double)) AS chest_received
FROM erc1155_polygon.evt_TransferSingle
WHERE contract_address = 0x57a9b65cEaC6c5ee0CEC8Cd4571a7c3805B558D8
AND CAST(id AS integer) = 16
-- AND CAST(id AS integer) >= 15
-- AND CAST(id AS integer) <= 19
AND "from" = 0x0000000000000000000000000000000000000000
GROUP BY 1,2

),


chests_opened AS (

SELECT
DATE_TRUNC('day', evt_block_time) AS time,
'open chest' AS action_type,
SUM(CAST(value AS double)) AS chest_opened
FROM erc1155_polygon.evt_TransferSingle
WHERE contract_address = 0x57a9b65cEaC6c5ee0CEC8Cd4571a7c3805B558D8
AND CAST(id AS integer) = 16
-- AND CAST(id AS integer) >= 15
-- AND CAST(id AS integer) <= 19
AND "to" = 0x0000000000000000000000000000000000000000
GROUP BY 1,2

)


SELECT
r.time,
r.chest_received,
o.chest_opened,
SUM (r.chest_received-o.chest_opened) OVER (ORDER BY r.time) AS net_chests_remaining
FROM chests_received r
JOIN chests_opened o
ON r.time = o.time
ORDER BY r.time DESC
