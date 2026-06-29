============================================================
TABELLE: project_gemeinden
============================================================
Spalten:
  - state_land                     string
  - district_kreis                 string
  - regional_designation           string
  - municipality_name              string
  - municipality_designation       string
  - area_km2                       double
  - population_total               bigint
  - male                           bigint
  - female                         bigint
  - per_km2                        double
  - postal_code                    string
  - longitude                      string
  - latitude                       string

Zeilen: 10950
Beispielzeilen:
  ('Schleswig-Holstein', '"Flensburg', ' Stadt"', 'Kreisfreie Stadt', 'Flensburg', None, None, 96431, 47553, 48878.0, '1700', '24937', '943751')
  ('Schleswig-Holstein', '"Kiel', ' Landeshauptstadt"', 'Kreisfreie Stadt', 'Kiel', None, None, 251751, 123080, 128671.0, '2122', '24103', '"10')
  ('Schleswig-Holstein', '"Lübeck', ' Hansestadt"', 'Kreisfreie Stadt', 'Lübeck', None, None, 217061, 104263, 112798.0, '1013', '23539', '"10')

============================================================
TABELLE: project_bauland
============================================================
Spalten:
  - jahr                           bigint
  - kreis_id                       string
  - kreis                          string
  - merkmal                        string
  - auspraegung                    string
  - insgesamt                      bigint
  - baureifes_land                 bigint

Zeilen: 21600
Beispielzeilen:
  (2024, 'DG', 'Deutschland', 'Ver�u�erungsfälle von Bauland', 'Anzahl', 39235, 32214)
  (2024, 'DG', 'Deutschland', 'Ver�u�erte Baulandfläche', '1000 qm', 71209, 28757)
  (2024, 'DG', 'Deutschland', 'Kaufsumme', 'Tsd. EUR', 10142837, 7195967)

============================================================
TABELLE: project_klimadaten
============================================================
Spalten:
  - dt                             string
  - averagetemperature             double
  - averagetemperatureuncertainty  double
  - city                           string
  - country                        string
  - latitude                       string
  - longitude                      string

Zeilen: 8599212
Beispielzeilen:
  ('1743-11-01', 6.068, 1.7369999999999999, 'Århus', 'Denmark', '57.05N', '10.33E')
  ('1743-12-01', None, None, 'Århus', 'Denmark', '57.05N', '10.33E')
  ('1744-01-01', None, None, 'Århus', 'Denmark', '57.05N', '10.33E')

============================================================
TABELLE: project_bevoelkerungzahlen
============================================================
Spalten:
  - id                             string
  - kreis                          string
  - insgesamt_24                   bigint
  - maennlich_24                   bigint
  - weiblich_24                    bigint
  - insgesamt_23                   bigint
  - maennlich_23                   bigint
  - weiblich_23                    bigint
  - insgesamt_22                   bigint
  - maennlich_22                   bigint
  - weiblich_22                    bigint
  - insgesamt_21                   bigint
  - maennlich_21                   bigint
  - weiblich_21                    bigint
  - insgesamt_20                   bigint
  - maennlich_20                   bigint
  - weiblich_20                    bigint
  - insgesamt_19                   bigint
  - maennlich_19                   bigint
  - weiblich_19                    bigint
  - insgesamt_18                   bigint
  - maennlich_18                   bigint
  - weiblich_18                    bigint
  - insgesamt_17                   bigint
  - maennlich_17                   bigint
  - weiblich_17                    bigint
  - insgesamt_16                   bigint
  - maennlich_16                   bigint
  - weiblich_16                    bigint
  - insgesamt_15                   bigint
  - maennlich_15                   bigint
  - weiblich_15                    bigint
  - insgesamt_14                   bigint
  - maennlich_14                   bigint
  - weiblich_14                    bigint
  - insgesamt_13                   bigint
  - maennlich_13                   bigint
  - weiblich_13                    bigint
  - insgesamt_12                   bigint
  - maennlich_12                   bigint
  - weiblich_12                    bigint
  - insgesamt_11                   bigint
  - maennlich_11                   bigint
  - weiblich_11                    bigint
  - insgesamt_10                   bigint
  - maennlich_10                   bigint
  - weiblich_10                    bigint
  - insgesamt_09                   bigint
  - maennlich_09                   bigint
  - weiblich_09                    bigint
  - insgesamt_08                   bigint
  - maennlich_08                   bigint
  - weiblich_08                    bigint
  - insgesamt_07                   bigint
  - maennlich_07                   bigint
  - weiblich_07                    bigint
  - insgesamt_06                   bigint
  - maennlich_06                   bigint
  - weiblich_06                    bigint
  - insgesamt_05                   bigint
  - maennlich_05                   bigint
  - weiblich_05                    bigint
  - insgesamt_04                   bigint
  - maennlich_04                   bigint
  - weiblich_04                    bigint
  - insgesamt_03                   bigint
  - maennlich_03                   bigint
  - weiblich_03                    bigint
  - insgesamt_02                   bigint
  - maennlich_02                   bigint
  - weiblich_02                    bigint
  - insgesamt_01                   bigint
  - maennlich_01                   bigint
  - weiblich_01                    bigint
  - insgesamt_00                   bigint
  - maennlich_00                   bigint
  - weiblich_00                    bigint
  - insgesamt_99                   bigint
  - maennlich_99                   bigint
  - weiblich_99                    bigint
  - insgesamt_98                   bigint
  - maennlich_98                   bigint
  - weiblich_98                    bigint
  - insgesamt_97                   bigint
  - maennlich_97                   bigint
  - weiblich_97                    bigint
  - insgesamt_96                   bigint
  - maennlich_96                   bigint
  - weiblich_96                    bigint
  - insgesamt_95                   bigint
  - maennlich_95                   bigint
  - weiblich_95                    bigint

Zeilen: 581
Beispielzeilen:
  ('DG', 'Deutschland', 83577140, 41241701, 42335439, 83456045, 41161931, 42294114, 83118501, 40919705, 42198796, 83237124, 41066785, 42170339, 83155031, 41026519, 42128512, 83166711, 41037613, 42129098, 83019213, 40966691, 42052522, 82792351, 40843565, 41948786, 82521653, 40697118, 41824535, 82175684, 40514123, 41661561, 81197537, 39835457, 41362080, 80767463, 39556923, 41210540, 80523746, 39380976, 41142770, 80327900, 39229947, 41097953, 81751602, 40112425, 41639177, 81802257, 40103606, 41698651, 82002356, 40184283, 41818073, 82217837, 40274292, 41943545, 82314906, 40301166, 42013740, 82437995, 40339961, 42098034, 82500849, 40353627, 42147222, 82531671, 40356014, 42175657, 82536680, 40344879, 42191801, 82440309, 40274676, 42165633, 82259540, 40156536, 42103004, 82163475, 40090776, 42072699, 82037011, 40004142, 42032869, 82057379, 39992311, 42065068, 82012162, 39954835, 42057327, 81817499, 39824823, 41992676)
  ('01', '  Schleswig-Holstein', 2959517, 1447902, 1511615, 2953202, 1443888, 1509314, 2939283, 1434715, 1504568, 2922005, 1431064, 1490941, 2910875, 1425649, 1485226, 2903773, 1422883, 1480890, 2896712, 1419457, 1477255, 2889821, 1416535, 1473286, 2881926, 1412665, 1469261, 2858714, 1399458, 1459256, 2830864, 1381451, 1449413, 2815955, 1372031, 1443924, 2806531, 1365954, 1440577, 2802266, 1362391, 1439875, 2834259, 1388912, 1445347, 2832027, 1387049, 1444978, 2834260, 1387798, 1446462, 2837373, 1388938, 1448435, 2834254, 1386770,1447484, 2832950, 1385285, 1447665, 2828760, 1382531, 1446229, 2823171, 1379707, 1443464, 2816507, 1376370, 1440137, 2804249, 1370626, 1433623, 2789761, 1363617, 1426144, 2777275, 1357398, 1419877, 2766057, 1351519, 1414538, 2756473, 1346729, 1409744, 2742293, 1339326, 1402967, 2725461, 1330257, 1395204)
  ('01001', '      Flensburg, kreisfreie Stadt', 96326, 47651, 48675, 96431, 47553, 48878, 96217, 47482, 48735, 91113, 45336, 45777, 89934, 44797, 45137, 90164, 44904, 45260, 89504, 44599, 44905, 88519, 44086, 44433, 87432, 43617, 43815, 85942, 42767, 43175, 84694, 41826, 42868, 83971, 41344, 42627, 83462, 41034, 42428, 82801, 40713, 42088, 88759, 43759, 45000, 88502, 43648, 44854, 88718, 43774, 44944, 87792, 43170, 44622, 86630, 42358, 44272, 86080, 41968, 44112, 85762, 41816, 43946, 85300, 41424, 43876, 84704, 40996, 43708, 84480, 40902, 43578, 84281, 40724, 43557, 84449, 40773, 43676, 84742, 40909, 43833, 85547, 41330, 44217, 86630, 41707, 44923, 87276, 41996, 45280)