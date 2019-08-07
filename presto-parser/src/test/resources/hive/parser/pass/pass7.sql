SELECT UID,
       sum(if(DAY>=date_sub('2019-07-30', 1)
              AND DAY<'2019-07-30', 1, 0)) AS enter_total_cnt_1d,
       sum(if(DAY>=date_sub('2019-07-30', 3)
              AND DAY<'2019-07-30', 1, 0)) AS enter_total_cnt_3d,
       sum(if(DAY>=date_sub('2019-07-30', 7)
              AND DAY<'2019-07-30', 1, 0)) AS enter_total_cnt_7d,
       sum(if(DAY>=date_sub('2019-07-30', 14)
              AND DAY<'2019-07-30', 1, 0)) AS enter_total_cnt_14d,
       sum(if(DAY>=date_sub('2019-07-30', 30)
              AND DAY<'2019-07-30', 1, 0)) AS enter_total_cnt_30d,
       sum(if(DAY>=date_sub('2019-07-30', 60)
              AND DAY<'2019-07-30', 1, 0)) AS enter_total_cnt_60d,
       0L AS enter_room_total_cnt_1d,
       0L AS enter_room_total_cnt_3d,
       0L AS enter_room_total_cnt_7d,
       0L AS enter_room_total_cnt_14d,
       0L AS enter_room_total_cnt_30d,
       0L AS enter_room_total_cnt_60d,
       0L AS enter_ratio_room_1d,
       0L AS enter_ratio_room_3d,
       0L AS enter_ratio_room_7d,
       0L AS enter_ratio_room_14d,
       0L AS enter_ratio_room_30d,
       0L AS enter_ratio_room_60d,
       0L AS enter_from_roomlist_cnt_1d,
       0L AS enter_from_roomlist_cnt_3d,
       0L AS enter_from_roomlist_cnt_7d,
       0L AS enter_from_roomlist_cnt_14d,
       0L AS enter_from_roomlist_cnt_30d,
       0L AS enter_from_roomlist_cnt_60d,
       0L AS enter_from_nearby_cnt_1d,
       0L AS enter_from_nearby_cnt_3d,
       0L AS enter_from_nearby_cnt_7d,
       0L AS enter_from_nearby_cnt_14d,
       0L AS enter_from_nearby_cnt_30d,
       0L AS enter_from_nearby_cnt_60d,
       0L AS enter_from_hot_cnt_1d,
       0L AS enter_from_hot_cnt_3d,
       0L AS enter_from_hot_cnt_7d,
       0L AS enter_from_hot_cnt_14d,
       0L AS enter_from_hot_cnt_30d,
       0L AS enter_from_hot_cnt_60d,
       sum(if(DAY>=date_sub('2019-07-30', 1)
              AND DAY<'2019-07-30'
              AND enter_way=3, 1, 0)) AS enter_from_profile_cnt_1d,
       sum(if(DAY>=date_sub('2019-07-30', 3)
              AND DAY<'2019-07-30'
              AND enter_way=3, 1, 0)) AS enter_from_profile_cnt_3d,
       sum(if(DAY>=date_sub('2019-07-30', 7)
              AND DAY<'2019-07-30'
              AND enter_way=3, 1, 0)) AS enter_from_profile_cnt_7d,
       sum(if(DAY>=date_sub('2019-07-30', 14)
              AND DAY<'2019-07-30'
              AND enter_way=3, 1, 0)) AS enter_from_profile_cnt_14d,
       sum(if(DAY>=date_sub('2019-07-30', 30)
              AND DAY<'2019-07-30'
              AND enter_way=3, 1, 0)) AS enter_from_profile_cnt_30d,
       sum(if(DAY>=date_sub('2019-07-30', 60)
              AND DAY<'2019-07-30'
              AND enter_way=3, 1, 0)) AS enter_from_profile_cnt_60d,
       sum(if(DAY>=date_sub('2019-07-30', 1)
              AND DAY<'2019-07-30'
              AND enter_way=4, 1, 0)) AS enter_from_buddy_cnt_1d,
       sum(if(DAY>=date_sub('2019-07-30', 3)
              AND DAY<'2019-07-30'
              AND enter_way=4, 1, 0)) AS enter_from_buddy_cnt_3d,
       sum(if(DAY>=date_sub('2019-07-30', 7)
              AND DAY<'2019-07-30'
              AND enter_way=4, 1, 0)) AS enter_from_buddy_cnt_7d,
       sum(if(DAY>=date_sub('2019-07-30', 14)
              AND DAY<'2019-07-30'
              AND enter_way=4, 1, 0)) AS enter_from_buddy_cnt_14d,
       sum(if(DAY>=date_sub('2019-07-30', 30)
              AND DAY<'2019-07-30'
              AND enter_way=4, 1, 0)) AS enter_from_buddy_cnt_30d,
       sum(if(DAY>=date_sub('2019-07-30', 60)
              AND DAY<'2019-07-30'
              AND enter_way=4, 1, 0)) AS enter_from_buddy_cnt_60d,
       0L AS enter_from_searchroom_cnt_1d,
       0L AS enter_from_searchroom_cnt_3d,
       0L AS enter_from_searchroom_cnt_7d,
       0L AS enter_from_searchroom_cnt_14d,
       0L AS enter_from_searchroom_cnt_30d,
       0L AS enter_from_searchroom_cnt_60d,
       0L AS enter_from_searchuser_cnt_1d,
       0L AS enter_from_searchuser_cnt_3d,
       0L AS enter_from_searchuser_cnt_7d,
       0L AS enter_from_searchuser_cnt_14d,
       0L AS enter_from_searchuser_cnt_30d,
       0L AS enter_from_searchuser_cnt_60d,
       0L AS enter_from_searchstranger_cnt_1d,
       0L AS enter_from_searchstranger_cnt_3d,
       0L AS enter_from_searchstranger_cnt_7d,
       0L AS enter_from_searchstranger_cnt_14d,
       0L AS enter_from_searchstranger_cnt_30d,
       0L AS enter_from_searchstranger_cnt_60d,
       0L AS enter_from_gamematch_cnt_1d,
       0L AS enter_from_gamematch_cnt_3d,
       0L AS enter_from_gamematch_cnt_7d,
       0L AS enter_from_gamematch_cnt_14d,
       0L AS enter_from_gamematch_cnt_30d,
       0L AS enter_from_gamematch_cnt_60d,
       0L AS enter_from_ktv_cnt_1d,
       0L AS enter_from_ktv_cnt_3d,
       0L AS enter_from_ktv_cnt_7d,
       0L AS enter_from_ktv_cnt_14d,
       0L AS enter_from_ktv_cnt_30d,
       0L AS enter_from_ktv_cnt_60d,
       0L AS enter_from_interest_cnt_1d,
       0L AS enter_from_interest_cnt_3d,
       0L AS enter_from_interest_cnt_7d,
       0L AS enter_from_interest_cnt_14d,
       0L AS enter_from_interest_cnt_30d,
       0L AS enter_from_interest_cnt_60d,
       0L AS exit_with_normal_cnt_1d,
       0L AS exit_with_normal_cnt_3d,
       0L AS exit_with_normal_cnt_7d,
       0L AS exit_with_normal_cnt_14d,
       0L AS exit_with_normal_cnt_30d,
       0L AS exit_with_normal_cnt_60d,
       sum(if(DAY>=date_sub('2019-07-30', 1)
              AND DAY<'2019-07-30'
              AND exit_reason=1, 1, 0)) AS exit_with_kickout_cnt_1d,
       sum(if(DAY>=date_sub('2019-07-30', 3)
              AND DAY<'2019-07-30'
              AND exit_reason=1, 1, 0)) AS exit_with_kickout_cnt_3d,
       sum(if(DAY>=date_sub('2019-07-30', 7)
              AND DAY<'2019-07-30'
              AND exit_reason=1, 1, 0)) AS exit_with_kickout_cnt_7d,
       sum(if(DAY>=date_sub('2019-07-30', 14)
              AND DAY<'2019-07-30'
              AND exit_reason=1, 1, 0)) AS exit_with_kickout_cnt_14d,
       sum(if(DAY>=date_sub('2019-07-30', 30)
              AND DAY<'2019-07-30'
              AND exit_reason=1, 1, 0)) AS exit_with_kickout_cnt_30d,
       sum(if(DAY>=date_sub('2019-07-30', 60)
              AND DAY<'2019-07-30'
              AND exit_reason=1, 1, 0)) AS exit_with_kickout_cnt_60d,
       sum(if(DAY>=date_sub('2019-07-30', 1)
              AND DAY<'2019-07-30'
              AND exit_reason=2, 1, 0)) AS exit_with_userreport_cnt_1d,
       sum(if(DAY>=date_sub('2019-07-30', 3)
              AND DAY<'2019-07-30'
              AND exit_reason=2, 1, 0)) AS exit_with_userreport_cnt_3d,
       sum(if(DAY>=date_sub('2019-07-30', 7)
              AND DAY<'2019-07-30'
              AND exit_reason=2, 1, 0)) AS exit_with_userreport_cnt_7d,
       sum(if(DAY>=date_sub('2019-07-30', 14)
              AND DAY<'2019-07-30'
              AND exit_reason=2, 1, 0)) AS exit_with_userreport_cnt_14d,
       sum(if(DAY>=date_sub('2019-07-30', 30)
              AND DAY<'2019-07-30'
              AND exit_reason=2, 1, 0)) AS exit_with_userreport_cnt_30d,
       sum(if(DAY>=date_sub('2019-07-30', 60)
              AND DAY<'2019-07-30'
              AND exit_reason=2, 1, 0)) AS exit_with_userreport_cnt_60d,
       0L AS exit_with_uikilled_cnt_1d,
       0L AS exit_with_uikilled_cnt_3d,
       0L AS exit_with_uikilled_cnt_7d,
       0L AS exit_with_uikilled_cnt_14d,
       0L AS exit_with_uikilled_cnt_30d,
       0L AS exit_with_uikilled_cnt_60d,
       0L AS exit_with_main_activity_finish_cnt_1d,
       0L AS exit_with_main_activity_finish_cnt_3d,
       0L AS exit_with_main_activity_finish_cnt_7d,
       0L AS exit_with_main_activity_finish_cnt_14d,
       0L AS exit_with_main_activity_finish_cnt_30d,
       0L AS exit_with_main_activity_finish_cnt_60d,
       0L AS exit_with_logout_cnt_1d,
       0L AS exit_with_logout_cnt_3d,
       0L AS exit_with_logout_cnt_7d,
       0L AS exit_with_logout_cnt_14d,
       0L AS exit_with_logout_cnt_30d,
       0L AS exit_with_logout_cnt_60d,
       date_sub('2019-07-30', 1) AS DAY
FROM algo.dm_bbd_user_enterroom_orc
WHERE DAY>=date_sub('2019-07-30', 30)
  AND DAY<'2019-07-30'
GROUP BY UID