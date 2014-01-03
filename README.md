Gravity charts project
======================

### Format of coucbase document: ###

key: sensor_number-timestamp_rounded to seconds
document: {1st number of 1st sec part: {1st num. of 1st subsec: [deviation, pressure],
1st num of second subsec: [deviation, pressure], ...},
          {1st number of 2nd sec part: {1st num. of 1st subsec: [deviation, pressure],
          1st num of second subsec: [deviation, pressure], ...}, ... }

### example: ###

key:1407838200
document: {0: {0: "25 256", 1: "35, 255", ...}, 1: {0: "26 256", 1: "46, 255", ...},

