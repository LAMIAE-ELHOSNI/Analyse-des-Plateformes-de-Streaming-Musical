-- Indexes for genres table
CREATE INDEX index_genres_id_genre ON genres (id_genre);

-- Indexes for gender table
CREATE INDEX index_gender_id_gender ON gender (id_gender);

-- Indexes for countries table
CREATE INDEX index_countries_id_country ON countries (id_country);

-- Indexes for tags table
CREATE INDEX index_tags_id_tag ON tags (id_tag);

-- Indexes for artists table
CREATE INDEX index_artists_artist_id ON artists (artist_id);

-- Indexes for artist_tags table
CREATE INDEX index_artist_tags_id_tag ON artist_tags (id_tag);
CREATE INDEX index_artist_tags_id_artist ON artist_tags (id_artist);

-- Indexes for albums_artists table
CREATE INDEX index_albums_artists_album_id ON albums_artists (album_id);
CREATE INDEX index_albums_artists_artist_id ON albums_artists (artist_id);

-- Indexes for gender_user table
CREATE INDEX index_gender_user_id ON gender_user (id);

-- Indexes for usage_period table
CREATE INDEX index_usage_period_id ON usage_period (id);

-- Indexes for listening_device table
CREATE INDEX index_listening_device_id ON listening_device (id);

-- Indexes for subscription_plan table
CREATE INDEX index_subscription_plan_id ON subscription_plan (id);

-- Indexes for sub_willingness table
CREATE INDEX index_sub_willingness_id ON sub_willingness (id);

-- Indexes for time_slot table
CREATE INDEX index_time_slot_id ON time_slot (id);

-- Indexes for lis_frequency table
CREATE INDEX index_lis_frequency_id ON lis_frequency (id);

-- Indexes for expl_method table
CREATE INDEX index_expl_method_id ON expl_method (id);

-- Indexes for pod_lis_frequency table
CREATE INDEX index_pod_lis_frequency_id ON pod_lis_frequency (id);

-- Indexes for pod_genre table
CREATE INDEX index_pod_genre_id ON pod_genre (id);

-- Indexes for pod_format table
CREATE INDEX index_pod_format_id ON pod_format (id);

-- Indexes for pod_duration table
CREATE INDEX index_pod_duration_id ON pod_duration (id);

-- Indexes for pod_variety_satisfaction table
CREATE INDEX index_pod_variety_satisfaction_id ON pod_variety_satisfaction (id);

-- Indexes for nationality table
CREATE INDEX index_nationality_id ON nationality (id);

-- Indexes for users_data table
CREATE INDEX index_users_data_id ON users_data (id);
CREATE INDEX index_users_data_Age ON users_data (Age);
CREATE INDEX index_users_data_Gender ON users_data (Gender);
CREATE INDEX index_users_data_spotify_usage_period ON users_data (spotify_usage_period);
CREATE INDEX index_users_data_spotify_listening_device ON users_data (spotify_listening_device);
CREATE INDEX index_users_data_spotify_subscription_plan ON users_data (spotify_subscription_plan);
CREATE INDEX index_users_data_premium_sub_willingness ON users_data (premium_sub_willingness);
CREATE INDEX index_users_data_fav_music_genre_id ON users_data (fav_music_genre_id);
CREATE INDEX index_users_data_music_time_slot ON users_data (music_time_slot);
CREATE INDEX index_users_data_music_lis_frequency ON users_data (music_lis_frequency);
CREATE INDEX index_users_data_music_expl_method ON users_data (music_expl_method);
CREATE INDEX index_users_data_music_recc_rating ON users_data (music_recc_rating);
CREATE INDEX index_users_data_pod_lis_frequency ON users_data (pod_lis_frequency);
CREATE INDEX index_users_data_fav_pod_genre ON users_data (fav_pod_genre);
CREATE INDEX index_users_data_preffered_pod_format ON users_data (preffered_pod_format);
CREATE INDEX index_users_data_preffered_pod_duration ON users_data (preffered_pod_duration);
CREATE INDEX index_users_data_pod_variety_satisfaction ON users_data (pod_variety_satisfaction);
CREATE INDEX index_users_data_date_registration ON users_data (date_registration);
CREATE INDEX index_users_data_nationality_id ON users_data (nationality_id);

-- Indexes for reviews table
CREATE INDEX index_reviews_id ON reviews (id);
CREATE INDEX index_reviews_Time_submitted ON reviews (Time_submitted);
CREATE INDEX index_reviews_Rating ON reviews (Rating);
CREATE INDEX index_reviews_Total_thumbsup ON reviews (Total_thumbsup);
CREATE INDEX index_reviews_date_registration ON reviews (date_registration);
CREATE INDEX index_reviews_user_id ON reviews (user_id);
