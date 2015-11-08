DROP TABLE Twitter;

CREATE TABLE Twitter
(
  tweet character varying(255),
  created_at timestamp without time zone,
  favorite_count integer,
  favorited boolean,
  filter_level character varying(255),
  id_str character varying(255),
  lang character varying(255),
  retweet_count integer,
  retweeted boolean,
  source character varying(255),
  timestamp_ms character varying(255),
  truncated boolean,
  user_description character varying(255),
  user_favourites_count integer,
  user_followers_count integer,
  user_friends_count integer,
  user_id_str character varying(255),
  user_location character varying(255),
  user_name character varying(255),
  user_profile_image_url character varying(255),
  user_screen_name character varying(255),
  user_statuses_count integer,
  user_time_zone character varying(255),
  hashtags character varying(255),
  urls character varying(255),
  user_timezone_country character varying(255),
  polarity double precision,
  sentiment character varying(255)
)
WITH (
  OIDS=FALSE
);
