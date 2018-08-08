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
from
np_accounts np
left join
bus_emails be
on
np.account_id = be.account_id),
-- where
-- case when np.account_id = be.account_id then 0 else 1 end = 1),

-- Dashboard pageviews
db_pv as (
select
pv.account_id
, count(distinct pv.created_at::date) as dashboard_daily_pageviews
from
  page_view pv
left join
chargify_signup_success css
  on
  pv.account_id::text = css.account_id::text
where
  (pv.created_at < css.created_at or css.created_at is null) and
  pv.created_at between (now() - interval '1 month') and now() and
  pv.url like '%dashboard.pusher.com%' and
  pv.url not like '%dashboard.pusher.com/account%' and
  isnumeric(pv.account_id) = TRUE
group by
  pv.account_id),

-- Cap dashboard views at 10 pageviews per month
db_pv_cap as (
select
account_id
, case when dashboard_daily_pageviews > 10 then 10 else dashboard_daily_pageviews end as dashboard_daily_pageviews
from
db_pv),


-- Join onto main table
db_accts as (
select
ba.account_id
, ba.bus_email_flag
, case when ba.account_id = db.account_id::int then db.dashboard_daily_pageviews else 0 end as dashboard_daily_pageviews
from
bus_accts ba
left join
db_pv_cap db
on
ba.account_id = db.account_id::int),

-- Find engagement frequency in past 30 days
eng_frq as (
select
account_id
, engagement_frequency
from
looker_scratch.lr$yzyxpdzpd30ss691eu6bh_user_engagement
where
month between now() - interval '1 month' and now()),

-- Cap engagement frequency to 6
eng_frq_cap as (
select
account_id
, case when engagement_frequency > 6 then 6 else engagement_frequency end as engagement_frequency
from
eng_frq),

-- Join onto main table
eng_accts as (
select
db.account_id
, db.bus_email_flag
, db.dashboard_daily_pageviews
, coalesce(ef.engagement_frequency, 0) as engagement_frequency
from
db_accts db
left join
eng_frq_cap ef
on
db.account_id::int = ef.account_id
),

-- Number of named apps in past month
is_named_app as (
select
account_id
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
created_at between now() - interval '1 month' and now()
group by
account_id
),

-- Cap named apps to 4
is_named_app_cap as (
select
account_id
, case when number_of_named_apps > 4 then 4 else number_of_named_apps end as number_of_named_apps
from
is_named_app),

-- Join onto main table
named_app_acct as (
select
ea.account_id
, ea.bus_email_flag
, ea.dashboard_daily_pageviews
, ea.engagement_frequency
, case when ea.account_id = ina.account_id then ina.number_of_named_apps else 0 end as number_of_named_apps
from
eng_accts ea
left join
is_named_app_cap ina
on
ea.account_id = ina.account_id),

-- Add tech info - score for frontend and backend language
language_active as (
select
ac.account_id
, case when ac.backend_tech is not null and ac.backend_tech <> 'none' then 1 else 0 end as backend_tech
, case when ac.frontend_tech is not null and ac.frontend_tech <> 'none' then 1 else 0 end as frontend_tech
, max(case
when  ac.backend_tech is null and ac.frontend_tech is null then 0
when ac.backend_tech = 'none' and ac.frontend_tech = 'none' then 0
else 1 end) as tech_info
from app_created ac
where
ac.created_at between now() - interval '1 month' and now()
group by
ac.account_id
, case when ac.backend_tech is not null and ac.backend_tech <> 'none' then 1 else 0 end
, case when ac.frontend_tech is not null and ac.frontend_tech <> 'none' then 1 else 0 end
),

language_active_agg as (
select
account_id
, max(backend_tech + frontend_tech) as tech_info
from language_active
group by
account_id),

--Join onto main table
ti_acct as (
select
na.account_id
, na.bus_email_flag
, na.dashboard_daily_pageviews
, na.engagement_frequency
, na.number_of_named_apps
, coalesce(la.tech_info, 0) as tech_info
from
named_app_acct na
left join
language_active_agg la
on
na.account_id = la.account_id),

-- Number of active apps
number_of_apps as (
select
account_id
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
created_at between now() - interval '1 month' and now()
order by
account_id
, app_id) as foo
group by
account_id),

-- Recode raw count to a flag
app_active as (
select
account_id
, case when number_of_active_apps > 0 then 1 else 0 end as active_app
from
number_of_apps),

-- join onto main table
na_accts as (
select
ti.account_id
, ti.bus_email_flag
, ti.dashboard_daily_pageviews
, ti.engagement_frequency
, ti.number_of_named_apps
, ti.tech_info
, case when na.active_app is not null then na.active_app else 0 end as active_app
from
ti_acct ti
left join
app_active na
on
ti.account_id = na.account_id),

-- Multiple environments
multiple_envs as (
select
ac.account_id
, max(case when ac.multiple_environments is not null then 1 else 0 end) as multiple_env_flag
from
app_created ac
where
created_at between now() - interval '1 month' and now()
group by
ac.account_id
),

-- Join onto main table
me_acct as (
select
na.account_id
, na.bus_email_flag
, na.dashboard_daily_pageviews
, na.engagement_frequency
, na.number_of_named_apps
, na.tech_info
, na.active_app
, coalesce(mef.multiple_env_flag, 0) as multiple_env_flag
from
na_accts na
left join
multiple_envs mef
on
na.account_id = mef.account_id),

-- Number of collaborators
collaborators as (
select
account_id
, count(distinct collaborator_email) as collaborators
from
app_collaborator_added
where
created_at between now() - interval '1 month' and now()
group by
account_id
),

-- Recode collaborators to flag
collaborator_flag as (
select
account_id
, case when collaborators > 0 then 1 else 0 end as collaborators
from
collaborators),

-- Join onto main table
col_acct as (
select
ma.account_id
, ma.bus_email_flag
, ma.dashboard_daily_pageviews
, ma.engagement_frequency
, ma. number_of_named_apps
, ma.tech_info
, ma.active_app
, ma.multiple_env_flag
, coalesce(cbi.collaborators, 0) as collaborators
from
  me_acct ma
left join
collaborator_flag cbi
 on
ma.account_id = cbi.account_id),

-- Tutorial views
tutorial_views as (
SELECT
channels_usage.id  AS account_id,
COUNT(DISTINCT CASE WHEN (page_views.web_asset = 'tutorials') THEN page_views.id  ELSE NULL END) AS tutorials_views
FROM LOOKER_SCRATCH_CHANNELS_TABLE /*LOOKER_SCRATCH.lr$yzfqup41m62e5n04u8xu_channels*/ AS channels_usage
FULL OUTER JOIN LOOKER_SCRATCH_SPLASH_PAGE_VIEWS_TABLE /*LOOKER_SCRATCH.lr$yzq8hiwlgpsgdr2y9umqg_splash_page_views*/ AS page_views ON (page_views.account_id::int) = channels_usage.id
FULL OUTER JOIN LOOKER_SCRATCH_WEB_SESSIONS_TABLE /*LOOKER_SCRATCH.lr$yzbrotenqsaj1ytjb2k1e_web_sessions*/ AS web_sessions ON web_sessions.account_id = channels_usage.id

WHERE (page_views.user_agent <> 'Pusher Web Analytics Bot' and (DATE(page_views.created_at )) <= now()::date) AND ((((channels_usage.created_at ) >= ((now() - interval '1 month')::date/*'2017-12-01'*/) AND (channels_usage.created_at ) < (now()::date/*'2018-01-28'*/)))) AND (web_sessions.session_created_at < web_sessions.account_created_at or web_sessions.account_created_at is null)
GROUP BY 1
ORDER BY 2 DESC),

-- Join onto main table
tv_acct as (
select
ca.account_id
, ca.bus_email_flag
, ca.dashboard_daily_pageviews
, ca.engagement_frequency
, ca. number_of_named_apps
, ca.tech_info
, ca.active_app
, ca.multiple_env_flag
, ca.collaborators
, case when tv.tutorials_views is null then 0 else tv.tutorials_views end as tutorial_views
from
col_acct ca
left join
tutorial_views tv
on
ca.account_id = tv.account_id),

-- How many docs views does each account have?
docs_pv as (
SELECT
channels_usage.id             AS account_id,
COUNT(DISTINCT page_views.id) AS page_views_count
FROM LOOKER_SCRATCH_CHANNELS_TABLE  /*LOOKER_SCRATCH.lr$yzfqup41m62e5n04u8xu_channels*/ AS channels_usage
FULL OUTER JOIN LOOKER_SCRATCH_SPLASH_PAGE_VIEWS_TABLE /*LOOKER_SCRATCH.lr$yzq8hiwlgpsgdr2y9umqg_splash_page_views*/ AS page_views
ON (page_views.account_id :: INT) = channels_usage.id
FULL OUTER JOIN  LOOKER_SCRATCH_WEB_SESSIONS_TABLE /*LOOKER_SCRATCH.lr$yzbrotenqsaj1ytjb2k1e_web_sessions*/ AS web_sessions
ON web_sessions.account_id = channels_usage.id

WHERE (page_views.user_agent <> 'Pusher Web Analytics Bot' AND (DATE(page_views.created_at)) <= now() :: DATE) AND
((((channels_usage.created_at) >= ((now() - interval '1 month')::date) AND
(channels_usage.created_at) < (now())))) AND
(((rtrim(page_views.url, '/')) LIKE '%docs%')) AND
(web_sessions.session_created_at < web_sessions.account_created_at OR
web_sessions.account_created_at IS NULL)
GROUP BY 1
ORDER BY 2 DESC
),

-- Join onto main table
doc_acct as (
select
ta.account_id
, ta.bus_email_flag
, ta.dashboard_daily_pageviews
, ta.engagement_frequency
, ta.number_of_named_apps
, ta.tech_info
, ta.active_app
, ta.multiple_env_flag
, ta.collaborators
, ta.tutorial_views
, case when pv.page_views_count is null then 0 else pv.page_views_count end as docs_page_views
from
 tv_acct ta
left join
  docs_pv pv
on
ta.account_id = pv.account_id),

-- Recode to pageview flag
doc_acct_flag as (
select
account_id
, bus_email_flag
, dashboard_daily_pageviews
, engagement_frequency
, number_of_named_apps
, tech_info
, active_app
, multiple_env_flag
, collaborators
, case when ((tutorial_views = 0 or docs_page_views > 0) or (tutorial_views > 0 and docs_page_views = 0) or (tutorial_views > 0 and docs_page_views > 0)) then 1 else 0 end as tut_doc_pv
from
doc_acct),


-- Connections soft limit accounts for past 30 days

connections_soft_limit_mon as (
select
account_id
from
connections_soft_limit
where
created_at between now() - interval '1 month' and now()
group by
account_id),


-- Connections soft limit (binary flag)
csl_acct as (
select
da.*
, case when da.account_id = cs.account_id then 1 else 0 end as con_soft_limit_flag
from
doc_acct_flag da
left join
connections_soft_limit_mon cs
on
da.account_id = cs.account_id
),


-- Messages soft limit accounts for past 30 days

messages_soft_limit_mon as (
select
account_id
from
messages_soft_limit
where
created_at between now() - interval '1 month' and now()
group by
account_id),


-- Messages soft limit (binary flag)
mes_acct as (
select
cs.*
, case when cs.account_id = mes.account_id then 1 else 0 end as mes_soft_limit_flag
from
csl_acct cs
left join
messages_soft_limit_mon as mes
on
cs.account_id = mes.account_id),

-- Recode to limit flag
mes_acct_flag as (
select
account_id
, bus_email_flag
, dashboard_daily_pageviews
, engagement_frequency
, number_of_named_apps
, tech_info
, active_app
, multiple_env_flag
, collaborators
, tut_doc_pv
, case when ((con_soft_limit_flag = 0 or mes_soft_limit_flag > 0) or (con_soft_limit_flag > 0 and mes_soft_limit_flag = 1) or (con_soft_limit_flag > 0 or mes_soft_limit_flag > 0)) then 1 else 0 end as hit_soft_limit
from
mes_acct),


-- Add email id for each account

scorecard_events as (
select
mes.account_id
, mes.bus_email_flag
, ac.email
, mes.dashboard_daily_pageviews
, mes.engagement_frequency
, mes.number_of_named_apps
, mes.tech_info
, mes.active_app
, mes.multiple_env_flag
, mes.collaborators
, mes.tut_doc_pv
, mes.hit_soft_limit
from
mes_acct_flag mes
left join
account ac
on
mes.account_id = ac.id),

-- Multiple columns by predefined weightings
weighted_scorecard as (
select
account_id
, bus_email_flag
, email
, (dashboard_daily_pageviews * 2) as dashboard_daily_pageviews
, (engagement_frequency * 2) as engagement_frequency
, (number_of_named_apps * 2) as number_of_named_apps
, (tech_info * 2) as tech_info
, (active_app * 2) as active_app
, multiple_env_flag
, collaborators
, tut_doc_pv
, hit_soft_limit
, (dashboard_daily_pageviews * 2) + (engagement_frequency * 2) + (number_of_named_apps * 2)
  + (tech_info * 2) + (active_app * 2) + multiple_env_flag + collaborators
  + tut_doc_pv + hit_soft_limit as total
from
scorecard_events),

-- Scale weights to be out of 100 (max score is 50 so multiply everything by 2)
scaled_scorecard as (
select
account_id
, bus_email_flag
, email
, (dashboard_daily_pageviews * 2) as dashboard_daily_pageviews
, (engagement_frequency * 2) as engagement_frequency
, (number_of_named_apps * 2) as number_of_named_apps
, (tech_info * 2) as tech_info
, (active_app * 2) as active_app
, (multiple_env_flag * 2) as multiple_env_flag
, (collaborators * 2) as collaborators
, (tut_doc_pv * 2) as tut_doc_pv
, (hit_soft_limit * 2) as hit_soft_limit
, (total * 2) as total
from weighted_scorecard)

select * from scaled_scorecard

;




