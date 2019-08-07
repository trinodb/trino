
SELECT platform,
       nettype,
       upper(countrycode),
       prefetchedms,
       entrytype,
       25 AS costtime_1,
       sum(if(sessionlogints>0
              AND sessionlogints<=25, 1, 0)) AS a_1,
       sum(if(medialogints>0
              AND medialogints<=25, 1, 0)) AS b_1,
       sum(if(msconnectedts>0
              AND msconnectedts<=25, 1, 0)) AS c_1,
       sum(if(vsconnectedts>0
              AND vsconnectedts<=25, 1, 0)) AS d_1,
       sum(if(firstiframets>0
              AND firstiframets<=25, 1, 0)) AS e_1,
       sum(if(firstvideopackts>0
              AND firstvideopackts<=25, 1, 0)) AS f_1,
       sum(if(firstiframeassemblets>0
              AND firstiframeassemblets<=25, 1, 0)) AS g_1,
       50 AS costtime_2,
       sum(if(sessionlogints>25
              AND sessionlogints<=50, 1, 0)) AS a_2,
       sum(if(medialogints>25
              AND medialogints<=50, 1, 0)) AS b_2,
       sum(if(msconnectedts>25
              AND msconnectedts<=50, 1, 0)) AS c_2,
       sum(if(vsconnectedts>25
              AND vsconnectedts<=50, 1, 0)) AS d_2,
       sum(if(firstiframets>25
              AND firstiframets<=50, 1, 0)) AS e_2,
       sum(if(firstvideopackts>25
              AND firstvideopackts<=50, 1, 0)) AS f_2,
       sum(if(firstiframeassemblets>25
              AND firstiframeassemblets<=50, 1, 0)) AS g_2,
       75 AS costtime_3,
       sum(if(sessionlogints>50
              AND sessionlogints<=75, 1, 0)) AS a_3,
       sum(if(medialogints>50
              AND medialogints<=75, 1, 0)) AS b_3,
       sum(if(msconnectedts>50
              AND msconnectedts<=75, 1, 0)) AS c_3,
       sum(if(vsconnectedts>50
              AND vsconnectedts<=75, 1, 0)) AS d_3,
       sum(if(firstiframets>50
              AND firstiframets<=75, 1, 0)) AS e_3,
       sum(if(firstvideopackts>50
              AND firstvideopackts<=75, 1, 0)) AS f_3,
       sum(if(firstiframeassemblets>50
              AND firstiframeassemblets<=75, 1, 0)) AS g_3,
       100 AS costtime_4,
       sum(if(sessionlogints>75
              AND sessionlogints<=100, 1, 0)) AS a_4,
       sum(if(medialogints>75
              AND medialogints<=100, 1, 0)) AS b_4,
       sum(if(msconnectedts>75
              AND msconnectedts<=100, 1, 0)) AS c_4,
       sum(if(vsconnectedts>75
              AND vsconnectedts<=100, 1, 0)) AS d_4,
       sum(if(firstiframets>75
              AND firstiframets<=100, 1, 0)) AS e_4,
       sum(if(firstvideopackts>75
              AND firstvideopackts<=100, 1, 0)) AS f_4,
       sum(if(firstiframeassemblets>75
              AND firstiframeassemblets<=100, 1, 0)) AS g_4,
       125 AS costtime_5,
       sum(if(sessionlogints>100
              AND sessionlogints<=125, 1, 0)) AS a_5,
       sum(if(medialogints>100
              AND medialogints<=125, 1, 0)) AS b_5,
       sum(if(msconnectedts>100
              AND msconnectedts<=125, 1, 0)) AS c_5,
       sum(if(vsconnectedts>100
              AND vsconnectedts<=125, 1, 0)) AS d_5,
       sum(if(firstiframets>100
              AND firstiframets<=125, 1, 0)) AS e_5,
       sum(if(firstvideopackts>100
              AND firstvideopackts<=125, 1, 0)) AS f_5,
       sum(if(firstiframeassemblets>100
              AND firstiframeassemblets<=125, 1, 0)) AS g_5,
       150 AS costtime_6,
       sum(if(sessionlogints>125
              AND sessionlogints<=150, 1, 0)) AS a_6,
       sum(if(medialogints>125
              AND medialogints<=150, 1, 0)) AS b_6,
       sum(if(msconnectedts>125
              AND msconnectedts<=150, 1, 0)) AS c_6,
       sum(if(vsconnectedts>125
              AND vsconnectedts<=150, 1, 0)) AS d_6,
       sum(if(firstiframets>125
              AND firstiframets<=150, 1, 0)) AS e_6,
       sum(if(firstvideopackts>125
              AND firstvideopackts<=150, 1, 0)) AS f_6,
       sum(if(firstiframeassemblets>125
              AND firstiframeassemblets<=150, 1, 0)) AS g_6,
       175 AS costtime_7,
       sum(if(sessionlogints>150
              AND sessionlogints<=175, 1, 0)) AS a_7,
       sum(if(medialogints>150
              AND medialogints<=175, 1, 0)) AS b_7,
       sum(if(msconnectedts>150
              AND msconnectedts<=175, 1, 0)) AS c_7,
       sum(if(vsconnectedts>150
              AND vsconnectedts<=175, 1, 0)) AS d_7,
       sum(if(firstiframets>150
              AND firstiframets<=175, 1, 0)) AS e_7,
       sum(if(firstvideopackts>150
              AND firstvideopackts<=175, 1, 0)) AS f_7,
       sum(if(firstiframeassemblets>150
              AND firstiframeassemblets<=175, 1, 0)) AS g_7,
       200 AS costtime_8,
       sum(if(sessionlogints>175
              AND sessionlogints<=200, 1, 0)) AS a_8,
       sum(if(medialogints>175
              AND medialogints<=200, 1, 0)) AS b_8,
       sum(if(msconnectedts>175
              AND msconnectedts<=200, 1, 0)) AS c_8,
       sum(if(vsconnectedts>175
              AND vsconnectedts<=200, 1, 0)) AS d_8,
       sum(if(firstiframets>175
              AND firstiframets<=200, 1, 0)) AS e_8,
       sum(if(firstvideopackts>175
              AND firstvideopackts<=200, 1, 0)) AS f_8,
       sum(if(firstiframeassemblets>175
              AND firstiframeassemblets<=200, 1, 0)) AS g_8,
       225 AS costtime_9,
       sum(if(sessionlogints>200
              AND sessionlogints<=225, 1, 0)) AS a_9,
       sum(if(medialogints>200
              AND medialogints<=225, 1, 0)) AS b_9,
       sum(if(msconnectedts>200
              AND msconnectedts<=225, 1, 0)) AS c_9,
       sum(if(vsconnectedts>200
              AND vsconnectedts<=225, 1, 0)) AS d_9,
       sum(if(firstiframets>200
              AND firstiframets<=225, 1, 0)) AS e_9,
       sum(if(firstvideopackts>200
              AND firstvideopackts<=225, 1, 0)) AS f_9,
       sum(if(firstiframeassemblets>200
              AND firstiframeassemblets<=225, 1, 0)) AS g_9,
       250 AS costtime_10,
       sum(if(sessionlogints>225
              AND sessionlogints<=250, 1, 0)) AS a_10,
       sum(if(medialogints>225
              AND medialogints<=250, 1, 0)) AS b_10,
       sum(if(msconnectedts>225
              AND msconnectedts<=250, 1, 0)) AS c_10,
       sum(if(vsconnectedts>225
              AND vsconnectedts<=250, 1, 0)) AS d_10,
       sum(if(firstiframets>225
              AND firstiframets<=250, 1, 0)) AS e_10,
       sum(if(firstvideopackts>225
              AND firstvideopackts<=250, 1, 0)) AS f_10,
       sum(if(firstiframeassemblets>225
              AND firstiframeassemblets<=250, 1, 0)) AS g_10,
       275 AS costtime_11,
       sum(if(sessionlogints>250
              AND sessionlogints<=275, 1, 0)) AS a_11,
       sum(if(medialogints>250
              AND medialogints<=275, 1, 0)) AS b_11,
       sum(if(msconnectedts>250
              AND msconnectedts<=275, 1, 0)) AS c_11,
       sum(if(vsconnectedts>250
              AND vsconnectedts<=275, 1, 0)) AS d_11,
       sum(if(firstiframets>250
              AND firstiframets<=275, 1, 0)) AS e_11,
       sum(if(firstvideopackts>250
              AND firstvideopackts<=275, 1, 0)) AS f_11,
       sum(if(firstiframeassemblets>250
              AND firstiframeassemblets<=275, 1, 0)) AS g_11,
       300 AS costtime_12,
       sum(if(sessionlogints>275
              AND sessionlogints<=300, 1, 0)) AS a_12,
       sum(if(medialogints>275
              AND medialogints<=300, 1, 0)) AS b_12,
       sum(if(msconnectedts>275
              AND msconnectedts<=300, 1, 0)) AS c_12,
       sum(if(vsconnectedts>275
              AND vsconnectedts<=300, 1, 0)) AS d_12,
       sum(if(firstiframets>275
              AND firstiframets<=300, 1, 0)) AS e_12,
       sum(if(firstvideopackts>275
              AND firstvideopackts<=300, 1, 0)) AS f_12,
       sum(if(firstiframeassemblets>275
              AND firstiframeassemblets<=300, 1, 0)) AS g_12,
       325 AS costtime_13,
       sum(if(sessionlogints>300
              AND sessionlogints<=325, 1, 0)) AS a_13,
       sum(if(medialogints>300
              AND medialogints<=325, 1, 0)) AS b_13,
       sum(if(msconnectedts>300
              AND msconnectedts<=325, 1, 0)) AS c_13,
       sum(if(vsconnectedts>300
              AND vsconnectedts<=325, 1, 0)) AS d_13,
       sum(if(firstiframets>300
              AND firstiframets<=325, 1, 0)) AS e_13,
       sum(if(firstvideopackts>300
              AND firstvideopackts<=325, 1, 0)) AS f_13,
       sum(if(firstiframeassemblets>300
              AND firstiframeassemblets<=325, 1, 0)) AS g_13,
       350 AS costtime_14,
       sum(if(sessionlogints>325
              AND sessionlogints<=350, 1, 0)) AS a_14,
       sum(if(medialogints>325
              AND medialogints<=350, 1, 0)) AS b_14,
       sum(if(msconnectedts>325
              AND msconnectedts<=350, 1, 0)) AS c_14,
       sum(if(vsconnectedts>325
              AND vsconnectedts<=350, 1, 0)) AS d_14,
       sum(if(firstiframets>325
              AND firstiframets<=350, 1, 0)) AS e_14,
       sum(if(firstvideopackts>325
              AND firstvideopackts<=350, 1, 0)) AS f_14,
       sum(if(firstiframeassemblets>325
              AND firstiframeassemblets<=350, 1, 0)) AS g_14,
       375 AS costtime_15,
       sum(if(sessionlogints>350
              AND sessionlogints<=375, 1, 0)) AS a_15,
       sum(if(medialogints>350
              AND medialogints<=375, 1, 0)) AS b_15,
       sum(if(msconnectedts>350
              AND msconnectedts<=375, 1, 0)) AS c_15,
       sum(if(vsconnectedts>350
              AND vsconnectedts<=375, 1, 0)) AS d_15,
       sum(if(firstiframets>350
              AND firstiframets<=375, 1, 0)) AS e_15,
       sum(if(firstvideopackts>350
              AND firstvideopackts<=375, 1, 0)) AS f_15,
       sum(if(firstiframeassemblets>350
              AND firstiframeassemblets<=375, 1, 0)) AS g_15,
       400 AS costtime_16,
       sum(if(sessionlogints>375
              AND sessionlogints<=400, 1, 0)) AS a_16,
       sum(if(medialogints>375
              AND medialogints<=400, 1, 0)) AS b_16,
       sum(if(msconnectedts>375
              AND msconnectedts<=400, 1, 0)) AS c_16,
       sum(if(vsconnectedts>375
              AND vsconnectedts<=400, 1, 0)) AS d_16,
       sum(if(firstiframets>375
              AND firstiframets<=400, 1, 0)) AS e_16,
       sum(if(firstvideopackts>375
              AND firstvideopackts<=400, 1, 0)) AS f_16,
       sum(if(firstiframeassemblets>375
              AND firstiframeassemblets<=400, 1, 0)) AS g_16,
       425 AS costtime_17,
       sum(if(sessionlogints>400
              AND sessionlogints<=425, 1, 0)) AS a_17,
       sum(if(medialogints>400
              AND medialogints<=425, 1, 0)) AS b_17,
       sum(if(msconnectedts>400
              AND msconnectedts<=425, 1, 0)) AS c_17,
       sum(if(vsconnectedts>400
              AND vsconnectedts<=425, 1, 0)) AS d_17,
       sum(if(firstiframets>400
              AND firstiframets<=425, 1, 0)) AS e_17,
       sum(if(firstvideopackts>400
              AND firstvideopackts<=425, 1, 0)) AS f_17,
       sum(if(firstiframeassemblets>400
              AND firstiframeassemblets<=425, 1, 0)) AS g_17,
       450 AS costtime_18,
       sum(if(sessionlogints>425
              AND sessionlogints<=450, 1, 0)) AS a_18,
       sum(if(medialogints>425
              AND medialogints<=450, 1, 0)) AS b_18,
       sum(if(msconnectedts>425
              AND msconnectedts<=450, 1, 0)) AS c_18,
       sum(if(vsconnectedts>425
              AND vsconnectedts<=450, 1, 0)) AS d_18,
       sum(if(firstiframets>425
              AND firstiframets<=450, 1, 0)) AS e_18,
       sum(if(firstvideopackts>425
              AND firstvideopackts<=450, 1, 0)) AS f_18,
       sum(if(firstiframeassemblets>425
              AND firstiframeassemblets<=450, 1, 0)) AS g_18,
       475 AS costtime_19,
       sum(if(sessionlogints>450
              AND sessionlogints<=475, 1, 0)) AS a_19,
       sum(if(medialogints>450
              AND medialogints<=475, 1, 0)) AS b_19,
       sum(if(msconnectedts>450
              AND msconnectedts<=475, 1, 0)) AS c_19,
       sum(if(vsconnectedts>450
              AND vsconnectedts<=475, 1, 0)) AS d_19,
       sum(if(firstiframets>450
              AND firstiframets<=475, 1, 0)) AS e_19,
       sum(if(firstvideopackts>450
              AND firstvideopackts<=475, 1, 0)) AS f_19,
       sum(if(firstiframeassemblets>450
              AND firstiframeassemblets<=475, 1, 0)) AS g_19,
       500 AS costtime_20,
       sum(if(sessionlogints>475
              AND sessionlogints<=500, 1, 0)) AS a_20,
       sum(if(medialogints>475
              AND medialogints<=500, 1, 0)) AS b_20,
       sum(if(msconnectedts>475
              AND msconnectedts<=500, 1, 0)) AS c_20,
       sum(if(vsconnectedts>475
              AND vsconnectedts<=500, 1, 0)) AS d_20,
       sum(if(firstiframets>475
              AND firstiframets<=500, 1, 0)) AS e_20,
       sum(if(firstvideopackts>475
              AND firstvideopackts<=500, 1, 0)) AS f_20,
       sum(if(firstiframeassemblets>475
              AND firstiframeassemblets<=500, 1, 0)) AS g_20,
       525 AS costtime_21,
       sum(if(sessionlogints>500
              AND sessionlogints<=525, 1, 0)) AS a_21,
       sum(if(medialogints>500
              AND medialogints<=525, 1, 0)) AS b_21,
       sum(if(msconnectedts>500
              AND msconnectedts<=525, 1, 0)) AS c_21,
       sum(if(vsconnectedts>500
              AND vsconnectedts<=525, 1, 0)) AS d_21,
       sum(if(firstiframets>500
              AND firstiframets<=525, 1, 0)) AS e_21,
       sum(if(firstvideopackts>500
              AND firstvideopackts<=525, 1, 0)) AS f_21,
       sum(if(firstiframeassemblets>500
              AND firstiframeassemblets<=525, 1, 0)) AS g_21,
       550 AS costtime_22,
       sum(if(sessionlogints>525
              AND sessionlogints<=550, 1, 0)) AS a_22,
       sum(if(medialogints>525
              AND medialogints<=550, 1, 0)) AS b_22,
       sum(if(msconnectedts>525
              AND msconnectedts<=550, 1, 0)) AS c_22,
       sum(if(vsconnectedts>525
              AND vsconnectedts<=550, 1, 0)) AS d_22,
       sum(if(firstiframets>525
              AND firstiframets<=550, 1, 0)) AS e_22,
       sum(if(firstvideopackts>525
              AND firstvideopackts<=550, 1, 0)) AS f_22,
       sum(if(firstiframeassemblets>525
              AND firstiframeassemblets<=550, 1, 0)) AS g_22,
       575 AS costtime_23,
       sum(if(sessionlogints>550
              AND sessionlogints<=575, 1, 0)) AS a_23,
       sum(if(medialogints>550
              AND medialogints<=575, 1, 0)) AS b_23,
       sum(if(msconnectedts>550
              AND msconnectedts<=575, 1, 0)) AS c_23,
       sum(if(vsconnectedts>550
              AND vsconnectedts<=575, 1, 0)) AS d_23,
       sum(if(firstiframets>550
              AND firstiframets<=575, 1, 0)) AS e_23,
       sum(if(firstvideopackts>550
              AND firstvideopackts<=575, 1, 0)) AS f_23,
       sum(if(firstiframeassemblets>550
              AND firstiframeassemblets<=575, 1, 0)) AS g_23,
       600 AS costtime_24,
       sum(if(sessionlogints>575
              AND sessionlogints<=600, 1, 0)) AS a_24,
       sum(if(medialogints>575
              AND medialogints<=600, 1, 0)) AS b_24,
       sum(if(msconnectedts>575
              AND msconnectedts<=600, 1, 0)) AS c_24,
       sum(if(vsconnectedts>575
              AND vsconnectedts<=600, 1, 0)) AS d_24,
       sum(if(firstiframets>575
              AND firstiframets<=600, 1, 0)) AS e_24,
       sum(if(firstvideopackts>575
              AND firstvideopackts<=600, 1, 0)) AS f_24,
       sum(if(firstiframeassemblets>575
              AND firstiframeassemblets<=600, 1, 0)) AS g_24,
       625 AS costtime_25,
       sum(if(sessionlogints>600
              AND sessionlogints<=625, 1, 0)) AS a_25,
       sum(if(medialogints>600
              AND medialogints<=625, 1, 0)) AS b_25,
       sum(if(msconnectedts>600
              AND msconnectedts<=625, 1, 0)) AS c_25,
       sum(if(vsconnectedts>600
              AND vsconnectedts<=625, 1, 0)) AS d_25,
       sum(if(firstiframets>600
              AND firstiframets<=625, 1, 0)) AS e_25,
       sum(if(firstvideopackts>600
              AND firstvideopackts<=625, 1, 0)) AS f_25,
       sum(if(firstiframeassemblets>600
              AND firstiframeassemblets<=625, 1, 0)) AS g_25,
       650 AS costtime_26,
       sum(if(sessionlogints>625
              AND sessionlogints<=650, 1, 0)) AS a_26,
       sum(if(medialogints>625
              AND medialogints<=650, 1, 0)) AS b_26,
       sum(if(msconnectedts>625
              AND msconnectedts<=650, 1, 0)) AS c_26,
       sum(if(vsconnectedts>625
              AND vsconnectedts<=650, 1, 0)) AS d_26,
       sum(if(firstiframets>625
              AND firstiframets<=650, 1, 0)) AS e_26,
       sum(if(firstvideopackts>625
              AND firstvideopackts<=650, 1, 0)) AS f_26,
       sum(if(firstiframeassemblets>625
              AND firstiframeassemblets<=650, 1, 0)) AS g_26,
       675 AS costtime_27,
       sum(if(sessionlogints>650
              AND sessionlogints<=675, 1, 0)) AS a_27,
       sum(if(medialogints>650
              AND medialogints<=675, 1, 0)) AS b_27,
       sum(if(msconnectedts>650
              AND msconnectedts<=675, 1, 0)) AS c_27,
       sum(if(vsconnectedts>650
              AND vsconnectedts<=675, 1, 0)) AS d_27,
       sum(if(firstiframets>650
              AND firstiframets<=675, 1, 0)) AS e_27,
       sum(if(firstvideopackts>650
              AND firstvideopackts<=675, 1, 0)) AS f_27,
       sum(if(firstiframeassemblets>650
              AND firstiframeassemblets<=675, 1, 0)) AS g_27,
       700 AS costtime_28,
       sum(if(sessionlogints>675
              AND sessionlogints<=700, 1, 0)) AS a_28,
       sum(if(medialogints>675
              AND medialogints<=700, 1, 0)) AS b_28,
       sum(if(msconnectedts>675
              AND msconnectedts<=700, 1, 0)) AS c_28,
       sum(if(vsconnectedts>675
              AND vsconnectedts<=700, 1, 0)) AS d_28,
       sum(if(firstiframets>675
              AND firstiframets<=700, 1, 0)) AS e_28,
       sum(if(firstvideopackts>675
              AND firstvideopackts<=700, 1, 0)) AS f_28,
       sum(if(firstiframeassemblets>675
              AND firstiframeassemblets<=700, 1, 0)) AS g_28,
       725 AS costtime_29,
       sum(if(sessionlogints>700
              AND sessionlogints<=725, 1, 0)) AS a_29,
       sum(if(medialogints>700
              AND medialogints<=725, 1, 0)) AS b_29,
       sum(if(msconnectedts>700
              AND msconnectedts<=725, 1, 0)) AS c_29,
       sum(if(vsconnectedts>700
              AND vsconnectedts<=725, 1, 0)) AS d_29,
       sum(if(firstiframets>700
              AND firstiframets<=725, 1, 0)) AS e_29,
       sum(if(firstvideopackts>700
              AND firstvideopackts<=725, 1, 0)) AS f_29,
       sum(if(firstiframeassemblets>700
              AND firstiframeassemblets<=725, 1, 0)) AS g_29,
       750 AS costtime_30,
       sum(if(sessionlogints>725
              AND sessionlogints<=750, 1, 0)) AS a_30,
       sum(if(medialogints>725
              AND medialogints<=750, 1, 0)) AS b_30,
       sum(if(msconnectedts>725
              AND msconnectedts<=750, 1, 0)) AS c_30,
       sum(if(vsconnectedts>725
              AND vsconnectedts<=750, 1, 0)) AS d_30,
       sum(if(firstiframets>725
              AND firstiframets<=750, 1, 0)) AS e_30,
       sum(if(firstvideopackts>725
              AND firstvideopackts<=750, 1, 0)) AS f_30,
       sum(if(firstiframeassemblets>725
              AND firstiframeassemblets<=750, 1, 0)) AS g_30,
       775 AS costtime_31,
       sum(if(sessionlogints>750
              AND sessionlogints<=775, 1, 0)) AS a_31,
       sum(if(medialogints>750
              AND medialogints<=775, 1, 0)) AS b_31,
       sum(if(msconnectedts>750
              AND msconnectedts<=775, 1, 0)) AS c_31,
       sum(if(vsconnectedts>750
              AND vsconnectedts<=775, 1, 0)) AS d_31,
       sum(if(firstiframets>750
              AND firstiframets<=775, 1, 0)) AS e_31,
       sum(if(firstvideopackts>750
              AND firstvideopackts<=775, 1, 0)) AS f_31,
       sum(if(firstiframeassemblets>750
              AND firstiframeassemblets<=775, 1, 0)) AS g_31,
       800 AS costtime_32,
       sum(if(sessionlogints>775
              AND sessionlogints<=800, 1, 0)) AS a_32,
       sum(if(medialogints>775
              AND medialogints<=800, 1, 0)) AS b_32,
       sum(if(msconnectedts>775
              AND msconnectedts<=800, 1, 0)) AS c_32,
       sum(if(vsconnectedts>775
              AND vsconnectedts<=800, 1, 0)) AS d_32,
       sum(if(firstiframets>775
              AND firstiframets<=800, 1, 0)) AS e_32,
       sum(if(firstvideopackts>775
              AND firstvideopackts<=800, 1, 0)) AS f_32,
       sum(if(firstiframeassemblets>775
              AND firstiframeassemblets<=800, 1, 0)) AS g_32,
       825 AS costtime_33,
       sum(if(sessionlogints>800
              AND sessionlogints<=825, 1, 0)) AS a_33,
       sum(if(medialogints>800
              AND medialogints<=825, 1, 0)) AS b_33,
       sum(if(msconnectedts>800
              AND msconnectedts<=825, 1, 0)) AS c_33,
       sum(if(vsconnectedts>800
              AND vsconnectedts<=825, 1, 0)) AS d_33,
       sum(if(firstiframets>800
              AND firstiframets<=825, 1, 0)) AS e_33,
       sum(if(firstvideopackts>800
              AND firstvideopackts<=825, 1, 0)) AS f_33,
       sum(if(firstiframeassemblets>800
              AND firstiframeassemblets<=825, 1, 0)) AS g_33,
       850 AS costtime_34,
       sum(if(sessionlogints>825
              AND sessionlogints<=850, 1, 0)) AS a_34,
       sum(if(medialogints>825
              AND medialogints<=850, 1, 0)) AS b_34,
       sum(if(msconnectedts>825
              AND msconnectedts<=850, 1, 0)) AS c_34,
       sum(if(vsconnectedts>825
              AND vsconnectedts<=850, 1, 0)) AS d_34,
       sum(if(firstiframets>825
              AND firstiframets<=850, 1, 0)) AS e_34,
       sum(if(firstvideopackts>825
              AND firstvideopackts<=850, 1, 0)) AS f_34,
       sum(if(firstiframeassemblets>825
              AND firstiframeassemblets<=850, 1, 0)) AS g_34,
       875 AS costtime_35,
       sum(if(sessionlogints>850
              AND sessionlogints<=875, 1, 0)) AS a_35,
       sum(if(medialogints>850
              AND medialogints<=875, 1, 0)) AS b_35,
       sum(if(msconnectedts>850
              AND msconnectedts<=875, 1, 0)) AS c_35,
       sum(if(vsconnectedts>850
              AND vsconnectedts<=875, 1, 0)) AS d_35,
       sum(if(firstiframets>850
              AND firstiframets<=875, 1, 0)) AS e_35,
       sum(if(firstvideopackts>850
              AND firstvideopackts<=875, 1, 0)) AS f_35,
       sum(if(firstiframeassemblets>850
              AND firstiframeassemblets<=875, 1, 0)) AS g_35,
       900 AS costtime_36,
       sum(if(sessionlogints>875
              AND sessionlogints<=900, 1, 0)) AS a_36,
       sum(if(medialogints>875
              AND medialogints<=900, 1, 0)) AS b_36,
       sum(if(msconnectedts>875
              AND msconnectedts<=900, 1, 0)) AS c_36,
       sum(if(vsconnectedts>875
              AND vsconnectedts<=900, 1, 0)) AS d_36,
       sum(if(firstiframets>875
              AND firstiframets<=900, 1, 0)) AS e_36,
       sum(if(firstvideopackts>875
              AND firstvideopackts<=900, 1, 0)) AS f_36,
       sum(if(firstiframeassemblets>875
              AND firstiframeassemblets<=900, 1, 0)) AS g_36,
       925 AS costtime_37,
       sum(if(sessionlogints>900
              AND sessionlogints<=925, 1, 0)) AS a_37,
       sum(if(medialogints>900
              AND medialogints<=925, 1, 0)) AS b_37,
       sum(if(msconnectedts>900
              AND msconnectedts<=925, 1, 0)) AS c_37,
       sum(if(vsconnectedts>900
              AND vsconnectedts<=925, 1, 0)) AS d_37,
       sum(if(firstiframets>900
              AND firstiframets<=925, 1, 0)) AS e_37,
       sum(if(firstvideopackts>900
              AND firstvideopackts<=925, 1, 0)) AS f_37,
       sum(if(firstiframeassemblets>900
              AND firstiframeassemblets<=925, 1, 0)) AS g_37,
       950 AS costtime_38,
       sum(if(sessionlogints>925
              AND sessionlogints<=950, 1, 0)) AS a_38,
       sum(if(medialogints>925
              AND medialogints<=950, 1, 0)) AS b_38,
       sum(if(msconnectedts>925
              AND msconnectedts<=950, 1, 0)) AS c_38,
       sum(if(vsconnectedts>925
              AND vsconnectedts<=950, 1, 0)) AS d_38,
       sum(if(firstiframets>925
              AND firstiframets<=950, 1, 0)) AS e_38,
       sum(if(firstvideopackts>925
              AND firstvideopackts<=950, 1, 0)) AS f_38,
       sum(if(firstiframeassemblets>925
              AND firstiframeassemblets<=950, 1, 0)) AS g_38,
       975 AS costtime_39,
       sum(if(sessionlogints>950
              AND sessionlogints<=975, 1, 0)) AS a_39,
       sum(if(medialogints>950
              AND medialogints<=975, 1, 0)) AS b_39,
       sum(if(msconnectedts>950
              AND msconnectedts<=975, 1, 0)) AS c_39,
       sum(if(vsconnectedts>950
              AND vsconnectedts<=975, 1, 0)) AS d_39,
       sum(if(firstiframets>950
              AND firstiframets<=975, 1, 0)) AS e_39,
       sum(if(firstvideopackts>950
              AND firstvideopackts<=975, 1, 0)) AS f_39,
       sum(if(firstiframeassemblets>950
              AND firstiframeassemblets<=975, 1, 0)) AS g_39,
       1000 AS costtime_40,
       sum(if(sessionlogints>975
              AND sessionlogints<=1000, 1, 0)) AS a_40,
       sum(if(medialogints>975
              AND medialogints<=1000, 1, 0)) AS b_40,
       sum(if(msconnectedts>975
              AND msconnectedts<=1000, 1, 0)) AS c_40,
       sum(if(vsconnectedts>975
              AND vsconnectedts<=1000, 1, 0)) AS d_40,
       sum(if(firstiframets>975
              AND firstiframets<=1000, 1, 0)) AS e_40,
       sum(if(firstvideopackts>975
              AND firstvideopackts<=1000, 1, 0)) AS f_40,
       sum(if(firstiframeassemblets>975
              AND firstiframeassemblets<=1000, 1, 0)) AS g_40,
       1025 AS costtime_41,
       sum(if(sessionlogints>=1000, 1, 0)) AS a_41,
       sum(if(medialogints>=1000, 1, 0)) AS b_41,
       sum(if(msconnectedts>=1000, 1, 0)) AS c_41,
       sum(if(vsconnectedts>=1000, 1, 0)) AS d_41,
       sum(if(firstiframets>=1000, 1, 0)) AS e_41,
       sum(if(firstvideopackts>=1000, 1, 0)) AS f_41,
       sum(if(firstiframeassemblets>=1000, 1, 0)) AS g_41
FROM bigolive.bigolive_audience_stats_orc
WHERE DAY='2019-07-30'
  AND length(countrycode)=2
GROUP BY platform,
         nettype,
         upper(countrycode),
         prefetchedms,
         entrytype