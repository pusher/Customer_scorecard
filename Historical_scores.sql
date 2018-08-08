-- Business customers who are not currently paying
with np_accounts as (
  select
id as account_id
from
account
where
plan_id = 1 and
email not like '%invalid%' and
email not like 'invalid%' and
email not like '%invalid' and
created_at between '01-Jan-2018' and now() ),

-- create a business email flag
-- all business emails in past 3 months
bus_emails as (
select
id as account_id
, email
, split_part(email, '@', 2) as domain
from
account
where
split_part(email, '@', 2) =
ANY('{.ac.uk
,.edu
,.se
,alice.it
,alunos.uminho.pt
,aol.com
,atlas.cz
,azet.sk
,blueyonder.co.uk
,bt.com
,btinternet.com
,chello.nl
,cin.ufpe.br
,com.tw
,email.com
,ex.ua
,fastmail.fm
,fh-bielefeld.de
,freenet.
,freeserve.co.uk
,games.com
,gamil.com
,gmai
,gmai.com
,gmail
,gmail.com
,gmx.co
,gmx.com
,gmx.de
,gmx.net
,gmx.net
,hanmail.net
,hanmail.net
,heroku.com
,hotmail
,hotmail.com
,hush.com
,hushmail.com
,icloud
,icloud.com
,icloud.com
,iname.com
,inbox.com
,itsligo.ie
,kcl.ac.uk
,laposte.net
,lavabit.com
,list.ru
,live.
,live.co.uk
,live.co.uk
,live.com
,love.com
,mac.com
,mail.com
,mail.ru
,me.com
,mohmal.com
,my.yorku.ca
,netcourrier.com
,nmmu.ac.za
,ntlworld.com
,o2.co.uk
,online.ua
,op.pl
,orange.
,orange.net
,outlook
,outlook.com
,pobox.com
,polimi.it
,pusher.com
,pusher.com
,qq.com
,queensu.ca
,rambler.ru
,rocketmail.com
,safe-mail.net
,sky.com
,talktalk.co.uk
,te.eg
,teilar.gr
,tiscali.co.uk
,tu-bs.de
,tut.by
,ua.pt
,ucm.es
,ufrj.br
,ufv.br
,ukr.net
,um.ac.id
,umontreal.ca
,univ-reims.fr
,uol.com
,virgin.net
,virginmedia.com
,wanadoo.co.uk
,web.de
,wow.com
,ya.ru
,yahoo
,yandex
,yandex.com
,yandex.ru
,ygm.com
,ymail.com
,zoho.com}')
),

-- Add a flag indicating if an account has a business email
bus_accts as (
select
np.account_id
, case when np.account_id = be.account_id then 0 else 1 end as bus_email_flag
, (generate_series(now() - interval '30 days', now(), '1 day'::interval))::date as end_date
from
np_accounts np
left join
bus_emails be
on
np.account_id = be.account_id
),

bus_accts_with_dates as (
select
account_id
, bus_email_flag
, (end_date - interval '30 days')::date as start_date
, end_date
from
bus_accts),

-- Dashboard daily pageviews
-- Pageviews
pageviews as (
select
pv.path
, pv.title
, pv.created_at
, pv.email
, pv.account_id
, pv.referrer
, pv.url
, pv.id
, pv.utc_datetime
, pv.anonymous_id
from
  page_view pv
left join
chargify_signup_success css
  on
  pv.account_id::text = css.account_id::text
where
  (pv.created_at < css.created_at or css.created_at is null) and
  pv.created_at between (now() - interval '60 days') and now() and
  pv.url like '%dashboard.pusher.com%' and
  pv.url not like '%dashboard.pusher.com/account%' and
  isnumeric(pv.account_id) = TRUE
),

-- Make a distinct list of dashboard login dates
pageview_login_dates as (
select
account_id
, created_at
from
pageviews
group by
account_id
,created_at),

-- Restrict to dashboard views
db_pv as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, sum(case when pv.created_at::date between ba.start_date and ba.end_date then 1 else 0 end) as dashboard_daily_pageviews
from
bus_accts_with_dates ba
left join
pageview_login_dates pv
on
ba.account_id = pv.account_id::int
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

-- Cap dashboard views at 10 pageviews per month
db_pv_cap as (
select
account_id
, bus_email_flag
, start_date
, end_date
, case when dashboard_daily_pageviews > 10 then 10 else dashboard_daily_pageviews end as dashboard_daily_pageviews
from
db_pv),

-- Db weighted scores
db_weight as
(select
account_id
, bus_email_flag
, start_date
, end_date
, (dashboard_daily_pageviews * 2) as dashboard_daily_pageviews
from
db_pv_cap
),

-- Engagement Frequency

-- Find engagement frequency in past 60 days
eng_frq as (
select
account_id
, month::date as month
, engagement_frequency
from
looker_scratch.lr$yzyxpdzpd30ss691eu6bh_user_engagement
where
month between now() - interval '60 days' and now()
order by account_id),

eng_accts as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, max(case when to_char(month, 'yyyy-mm') = to_char(end_date, 'yyyy-mm') then engagement_frequency else 0 end) as engagement_frequency
from
bus_accts_with_dates ba
left join
eng_frq ef
on
ba.account_id::int = ef.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
),

-- Cap engagement frequency to 6
eng_frq_cap as (
select
account_id
, bus_email_flag
, start_date
, end_date
, case when engagement_frequency > 6 then 6 else engagement_frequency end as engagement_frequency
from
eng_accts),

eng_weight as
  (select
account_id
, bus_email_flag
, start_date
, end_date
, (engagement_frequency * 2) as engagement_frequency
from
eng_frq_cap),

-- Number of named apps


-- Number of named apps in past month
is_named_app as (
select
account_id
, created_at::date
, sum(case when split_part(app_name, '-', 1) ~ 'able|action|active|actual|adept|adored|adroit|affectionate|agile|airy|alert|alive|alter|amiable|ample|and|anima|apt|ardent|are|astute|august|avid|awake|aware|balmy|benevolent|big|billowing|blessed|bold|boss|brainy|brave|brawny|breezy|brief|bright|brisk|busy|calm|can|canny|cared|caring|casual|celestial|charming|chic|chief|choice|chosen|chummy|civic|civil|classy|clean|clear|clever|close|cogent|composed|condemned|cool|cosmic|cozy|cuddly|cute|dainty|dandy|dapper|daring|dear|decent|deep|deft|deluxe|devout|direct|divine|doted|doting|dreamy|driven|dry|earthy|easy|elated|end|energized|enigmatic|equal|exact|exotic|expert|exuberant|fair|famed|famous|fancy|fast|fiery|fine|fit|flashy|fleet|flowing|fluent|fluffy|fluttering|flying|fond|for|frank|free|fresh|frightened|full|fun|funny|fuscia|gas|genial|gentle|giddy|gifted|giving|glad|gnarly|gold|golden|good|goodly|graceful|grand|greasy|great|green|grieving|groovy|guided|gutsy|haloed|happy|hardy|harmonious|hearty|heroic|high|hip|hollow|holy|homeless|honest|huge|human|humane|humble|hunky|icy|ideal|immune|indigo|inquisitive|jazzed|jazzy|jolly|jovial|joyful|joyous|jubilant|juicy|just|keen|khaki|kind|kingly|large|lavish|lawful|left|legal|legit|light|like|liked|likely|limber|limitless|lively|loved|lovely|loyal|lucid|lucky|lush|main|major|master|mature|max|maxed|mellow|merciful|merry|mighty|mint|mirthful|modern|modest|money|moonlit|moral|moving|mucho|mutual|mysterious|native|natural|near|neat|needed|new|nice|nifty|nimble|noble|normal|noted|novel|okay|open|outrageous|overt|pacific|parched|peachy|peppy|pithy|placid|pleasant|plucky|plum|poetic|poised|polite|posh|potent|pretty|prime|primo|prized|pro|prompt|proper|proud|pumped|punchy|pure|purring|quaint|quick|quiet|rad|radioactive|rapid|rare|reach|ready|real|regal|resilient|rich|right|robust|rooted|rosy|rugged|safe|sassy|saucy|savvy|scenic|screeching|secret|seemly|sensitive|serene|sharp|showy|shrewd|simple|sleek|slick|smart|smiley|smooth|snappy|snazzy|snowy|snugly|social|sole|solitary|sound|spacial|spicy|spiffy|spry|stable|star|stark|steady|stoic|strong|stunning|sturdy|suave|subtle|sunny|sunset|super|superb|sure|swank|sweet|swell|swift|talented|teal|the|thriving|tidy|timely|top|tops|tough|touted|tranquil|trim|tropical|true|trusty|try|undisturbed|unique|united|unsightly|unwavering|upbeat|uplifting|urbane|usable|useful|utmost|valid|vast|vestal|viable|vital|vivid|vocal|vogue|voiceless|volant|wandering|wanted|warm|wealthy|whispering|whole|winged|wired|wise|witty|wooden|worthy|zealous'
and
split_part(app_name, '-', 2) ~ 'abyss|animal|apple|atoll|aurora|autumn|bacon|badlands|ball|banana|bath|beach|bear|bed|bee|bike|bird|boat|book|bowl|branch|bread|breeze|briars|brook|brush|bunny|candy|canopy|canyon|car|cat|cave|cavern|cereal|chair|chasm|chip|cliff|coal|coast|cookie|cove|cow|crater|creek|darkness|dawn|desert|dew|dog|door|dove|drylands|duck|dusk|earth|fall|farm|fern|field|firefly|fish|fjord|flood|flower|flowers|fog|foliage|forest|freeze|frog|fu|galaxy|garden|geyser|gift|glass|grove|guide|guru|hat|hug|hero|hill|horse|house|hurricane|ice|iceberg|island|juice|lagoon|lake|land|lawn|leaf|leaves|light|lion|marsh|meadow|milk|mist|moon|moss|mountain|mouse|nature|oasis|ocean|pants|peak|pebble|pine|pilot|plane|planet|plant|plateau|pond|prize|rabbit|rain|range|reef|reserve|resonance|river|rock|sage|salute|sanctuary|sand|sands|shark|shelter|shirt|shoe|silence|sky|smokescreen|snowflake|socks|soil|soul|soup|sparrow|spoon|spring|star|stone|storm|stream|summer|summit|sun|sunrise|sunset|sunshine|surf|swamp|table|teacher|temple|thorns|tiger|tigers|towel|train|tree|truck|tsunami|tundra|valley|volcano|water|waterfall|waves|wild|willow|window|winds|winter|zebra'
and
split_part(app_name, '-', 3) is not null then 0
else
1 end) as number_of_named_apps
from
app_created
where
created_at between now() - interval '60 days' and now()
group by
account_id
, created_at
),

named_app_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, sum(case when ina.created_at between ba.start_date and ba.end_date then ina.number_of_named_apps else 0 end) as number_of_named_apps
from
bus_accts_with_dates ba
left join
is_named_app ina
on
ba.account_id = ina.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

-- Cap named apps to 4
is_named_app_cap as (
select
account_id
, bus_email_flag
, start_date
, end_date
, case when number_of_named_apps > 4 then 4 else number_of_named_apps end as number_of_named_apps
from
named_app_acct),

named_apps_weight as
  (select
account_id
, bus_email_flag
, start_date
, end_date
, (number_of_named_apps * 2) as number_of_named_apps
from
is_named_app_cap ),


-- Add tech info - score for frontend and backend language


language_active as (
select
ac.account_id
, ac.created_at::date
, case when ac.backend_tech is not null and ac.backend_tech <> 'none' then 1 else 0 end as backend_tech
, case when ac.frontend_tech is not null and ac.frontend_tech <> 'none' then 1 else 0 end as frontend_tech
, max(case
when  ac.backend_tech is null and ac.frontend_tech is null then 0
when ac.backend_tech = 'none' and ac.frontend_tech = 'none' then 0
else 1 end) as tech_info
from app_created ac
where
ac.created_at between now() - interval '60 days' and now()
group by
ac.account_id
, ac.created_at
, case when ac.backend_tech is not null and ac.backend_tech <> 'none' then 1 else 0 end
, case when ac.frontend_tech is not null and ac.frontend_tech <> 'none' then 1 else 0 end
),

language_active_agg as (
select
account_id
, created_at
, max(backend_tech + frontend_tech) as tech_info
from language_active
group by
account_id
, created_at),


ti_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, max(case when la.created_at between ba.start_date and ba.end_date then la.tech_info else 0 end) as tech_info
from
bus_accts_with_dates ba
left join
language_active_agg la
on
ba.account_id = la.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

ti_weight as (
select
account_id
, bus_email_flag
, start_date
, end_date
, (tech_info * 2) as tech_info
from
ti_acct),


-- Number of active apps
number_of_apps as (
select
account_id
, created_at::date
, count(distinct app_id) as number_of_active_apps
from
(
select
account_id
, app_id
, created_at
, max_connections
, all_messages
, case when max_connections >= 5 and all_messages >= 100 then 1 else 0 end as active_flag
from
daily_logs
where
case when max_connections >= 5 and all_messages >= 100 then 1 else 0 end = 1 and
created_at between now() - interval '60 days' and now()
order by
account_id
, app_id) as foo
group by
account_id
, created_at),

-- Recode raw count to a flag
app_active as (
select
account_id
, created_at
, case when number_of_active_apps > 0 then 1 else 0 end as active_app
from
number_of_apps),


na_accts as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, max(case when na.created_at between ba.start_date and ba.end_date then na.active_app else 0 end) as active_app
from
bus_accts_with_dates ba
left join
app_active na
on
ba.account_id = na.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

active_app_weight as (
select
account_id
, bus_email_flag
, start_date
, end_date
, (active_app * 2) as active_app
from
  na_accts),


-- Multiple environments
multiple_envs as (
select
ac.account_id
, ac.created_at::date
, max(case when ac.multiple_environments is not null then 1 else 0 end) as multiple_env_flag
from
app_created ac
where
created_at between now() - interval '60 days' and now()
group by
ac.account_id
,ac.created_at
),


me_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, max(case when mef.created_at between ba.start_date and ba.end_date then mef.multiple_env_flag else 0 end) as multiple_env_flag
from
bus_accts_with_dates ba
left join
multiple_envs mef
on
ba.account_id = mef.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

me_acct_weight as (
select
account_id
, bus_email_flag
, start_date
, end_date
, multiple_env_flag
from
me_acct
order by
account_id
),

-- Number of collaborators
collaborators as (
select
account_id
, created_at::date
, count(distinct collaborator_email) as collaborators
from
app_collaborator_added
where
created_at between now() - interval '60 days' and now()
group by
account_id
, created_at
),


-- Join onto main table
col_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, sum(case when cbi.created_at between ba.start_date and ba.end_date then cbi.collaborators else 0 end) as collaborators
from
  bus_accts_with_dates ba
left join
collaborators cbi
 on
ba.account_id = cbi.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),


-- Recode collaborators to flag and weight
collaborator_weight as (
select
account_id
, bus_email_flag
, start_date
, end_date
, case when collaborators > 0 then 1 else 0 end as collaborators
from
col_acct
order by
account_id),

-- Tutorial views
tutorial_views as (
SELECT
channels_usage.id  AS account_id,
page_views.created_at::date as created_at,
COUNT(DISTINCT CASE WHEN (page_views.web_asset = 'tutorials') THEN page_views.id  ELSE NULL END) AS tutorials_views
FROM LOOKER_SCRATCH_CHANNELS_TABLE /*LOOKER_SCRATCH.lr$yzh2klvz26zy95kv0x3hb_channels*/ AS channels_usage
FULL OUTER JOIN LOOKER_SCRATCH_SPLASH_PAGE_VIEWS_TABLE /*LOOKER_SCRATCH.lr$yzq8hiwlgpsgdr2y9umqg_splash_page_views*/ AS page_views ON (page_views.account_id::int) = channels_usage.id
FULL OUTER JOIN LOOKER_SCRATCH_WEB_SESSIONS_TABLE /*LOOKER_SCRATCH.lr$yzm9m39d1u9tpkvf8y0hg_web_sessions*/ AS web_sessions ON web_sessions.account_id = channels_usage.id

WHERE (page_views.user_agent <> 'Pusher Web Analytics Bot' and (DATE(page_views.created_at )) <= now()::date) AND ((((channels_usage.created_at ) >= ((now() - interval '60 days')::date/*'2017-12-01'*/) AND (channels_usage.created_at ) < (now()::date/*'2018-01-28'*/)))) AND (web_sessions.session_created_at < web_sessions.account_created_at or web_sessions.account_created_at is null)
GROUP BY 1, 2
ORDER BY 2 DESC),

-- Join onto main table
tv_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, sum(case when tv.created_at between ba.start_date and ba.end_date then tv.tutorials_views else 0 end) as tutorial_views
from
bus_accts_with_dates ba
left join
tutorial_views tv
on
ba.account_id = tv.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

-- How many docs views does each account have?
docs_pv as (
SELECT
channels_usage.id             AS account_id,
page_views.created_at::date as created_at,
COUNT(DISTINCT page_views.id) AS page_views_count
FROM LOOKER_SCRATCH_CHANNELS_TABLE  /*LOOKER_SCRATCH.lr$yzh2klvz26zy95kv0x3hb_channels*/ AS channels_usage
FULL OUTER JOIN LOOKER_SCRATCH_SPLASH_PAGE_VIEWS_TABLE /*LOOKER_SCRATCH.lr$yzq8hiwlgpsgdr2y9umqg_splash_page_views*/ AS page_views
ON (page_views.account_id :: INT) = channels_usage.id
FULL OUTER JOIN  LOOKER_SCRATCH_WEB_SESSIONS_TABLE /*LOOKER_SCRATCH.lr$yzm9m39d1u9tpkvf8y0hg_web_sessions*/ AS web_sessions
ON web_sessions.account_id = channels_usage.id

WHERE (page_views.user_agent <> 'Pusher Web Analytics Bot' AND (DATE(page_views.created_at)) <= now() :: DATE) AND
((((channels_usage.created_at) >= ((now() - interval '60 days')::date) AND
(channels_usage.created_at) < (now())))) AND
(((rtrim(page_views.url, '/')) LIKE '%docs%')) AND
(web_sessions.session_created_at < web_sessions.account_created_at OR
web_sessions.account_created_at IS NULL)
GROUP BY 1, 2
ORDER BY 2 DESC
),

-- Join onto main table
doc_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, sum(case when dp.created_at between ba.start_date and ba.end_date then dp.page_views_count else 0 end) as docs_page_views
from
bus_accts_with_dates ba
left join
docs_pv dp
on
ba.account_id = dp.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),

-- Join tutorial and doc pageviews
tv_doc_accts as (
select
ta.account_id
, ta.bus_email_flag
, ta.start_date
, ta.end_date
, ta.tutorial_views
, da.docs_page_views
from
tv_acct ta
full join
doc_acct da
on
ta.account_id = da.account_id
and
ta.bus_email_flag = da.bus_email_flag
and
ta.start_date = da.start_date
and
ta.end_date = da.end_date),


-- Recode to pageview flag and weight
doc_acct_weight as (
select
account_id
, bus_email_flag
, start_date
, end_date
, case when ((tutorial_views = 0 or docs_page_views > 0) or (tutorial_views > 0 and docs_page_views = 0) or (tutorial_views > 0 and docs_page_views > 0)) then 1 else 0 end as tut_doc_pv
from
tv_doc_accts),

-- Connections soft limit accounts for past 60 days

connections_soft_limit_mon as (
select
account_id
, created_at::date as created_at
from
connections_soft_limit
where
created_at between now() - interval '60 days' and now()
group by
account_id
, created_at),


-- Connections soft limit (binary flag)
csl_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, max(case when cs.created_at between ba.start_date and ba.end_date then 1 else 0 end) as con_soft_limit_flag
from
bus_accts_with_dates ba
left join
connections_soft_limit_mon cs
on
ba.account_id = cs.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
),


-- Messages soft limit accounts for past 30 days

messages_soft_limit_mon as (
select
account_id
, created_at::date
from
messages_soft_limit
where
created_at between now() - interval '60 days' and now()
group by
account_id
, created_at),


-- Messages soft limit (binary flag)
mes_acct as (
select
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date
, max(case when mes.created_at between ba.start_date and ba.end_date then 1 else 0 end) as mes_soft_limit_flag
from
bus_accts_with_dates ba
left join
messages_soft_limit_mon mes
on
ba.account_id = mes.account_id
group by
ba.account_id
, ba.bus_email_flag
, ba.start_date
, ba.end_date),


-- Join connection and messages soft limits
con_mes_lim as (
select
ca.account_id
, ca.bus_email_flag
, ca.start_date
, ca.end_date
, ca.con_soft_limit_flag
, mes.mes_soft_limit_flag
from
csl_acct ca
full join
mes_acct mes
on
ca.account_id = mes.account_id
and
ca.bus_email_flag = mes.bus_email_flag
and
ca.start_date = mes.start_date
and
ca.end_date = mes.end_date),

-- Recode to limit flag
soft_limit_weight as (
select
account_id
, bus_email_flag
, start_date
, end_date
, case when ((con_soft_limit_flag = 0 or mes_soft_limit_flag > 0) or (con_soft_limit_flag > 0 and mes_soft_limit_flag = 1) or (con_soft_limit_flag > 0 or mes_soft_limit_flag > 0)) then 1 else 0 end as hit_soft_limit
from
con_mes_lim),

-- Join all weighted tables and sum to obtain historical score

-- Layer weighted tables

-- Dashboard and engagement
hist_comp_weight as
(select
db.account_id
, db.bus_email_flag
, db.start_date
, db.end_date
, db.dashboard_daily_pageviews
, coalesce(ef.engagement_frequency, 0) as engagement_frequency
, coalesce(naw.number_of_named_apps, 0) as number_of_named_apps
, coalesce(tw.tech_info, 0) as tech_info
, coalesce(aaw.active_app, 0) as active_app
, coalesce(mew.multiple_env_flag, 0) as multiple_env_flag
, coalesce(cw.collaborators, 0) as collaborators
, coalesce(daw.tut_doc_pv, 0) as tut_doc_pv
, coalesce(slw.hit_soft_limit, 0) as hit_soft_limit
from
db_weight db
left join
eng_weight ef
on
db.account_id = ef.account_id
and
db.bus_email_flag = ef.bus_email_flag
and
db.start_date = ef.start_date
and
db.end_date = ef.end_date
left join
named_apps_weight naw
on
db.account_id = naw.account_id
and
db.bus_email_flag = naw.bus_email_flag
and
db.start_date = naw.start_date
and
db.end_date = naw.end_date
left join
ti_weight tw
on
db.account_id = tw.account_id
and
db.bus_email_flag = tw.bus_email_flag
and
db.start_date = tw.start_date
and
db.end_date = tw.end_date
left join
active_app_weight aaw
on
db.account_id = aaw.account_id
and
db.bus_email_flag = aaw.bus_email_flag
and
db.start_date = aaw.start_date
and
db.end_date = aaw.end_date
left join
me_acct_weight mew
on
db.account_id = mew.account_id
and
db.bus_email_flag = mew.bus_email_flag
and
db.start_date = mew.start_date
and
db.end_date = mew.end_date
left join
collaborator_weight cw
on
db.account_id = cw.account_id
and
db.bus_email_flag = cw.bus_email_flag
and
db.start_date = cw.start_date
and
db.end_date = cw.end_date
left join
doc_acct_weight daw
on
db.account_id = daw.account_id
and
db.bus_email_flag = daw.bus_email_flag
and
db.start_date = daw.start_date
and
db.end_date = daw.end_date
left join
soft_limit_weight slw
on
db.account_id = slw.account_id
and
db.bus_email_flag = slw.bus_email_flag
and
db.start_date = slw.start_date
and
db.end_date = slw.end_date),

hist_scores as (
select
account_id
, bus_email_flag
, start_date
, end_date
, (dashboard_daily_pageviews) + (engagement_frequency) + (number_of_named_apps)
  + (tech_info) + (active_app) + multiple_env_flag + collaborators
  + tut_doc_pv + hit_soft_limit as total
from hist_comp_weight),

-- Add email id
hist_score_email as (
select
hs.account_id
, ac.email
, hs.bus_email_flag
, hs.start_date
, hs.end_date
, hs.total as score
from
hist_scores hs
left join
account ac
on
hs.account_id = ac.id),

-- Scale scores
scaled_hist_score as (
select
account_id
, email
, bus_email_flag
, start_date
, end_date
, (score * 2) as score
from
hist_score_email
)

select * from scaled_hist_score

;












