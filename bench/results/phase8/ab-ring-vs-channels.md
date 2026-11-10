# ring vs channels, interleaved (5 pairs, 10,000 orders/s, 30 s measured, Kafka log in the path)

| tag | run | p50 us | p90 us | p99 us | p99.9 us | max us | GC cycles | publish lag p50 / p99 (ms, bucket bound) |
|---|---|---|---|---|---|---|---|---|
| ab-ring | 1 | 80 | 133 | 618 | 50079 | 105727 | 26 | 0.10 / 6.40 |
| ab-ring | 1 | 78 | 132 | 375 | 6623 | 16511 | 26 | 3.20 / 6.40 |
| ab-ring | 1 | 87 | 159 | 4755 | 53951 | 78911 | 26 | 3.20 / 12.80 |
| ab-ring | 1 | 80 | 135 | 444 | 11855 | 37343 | 26 | 3.20 / 25.60 |
| ab-ring | 1 | 76 | 129 | 283 | 2044 | 9903 | 26 | 3.20 / 6.40 |
| ab-chan | 1 | 63 | 127 | 1140 | 19711 | 42143 | 24 | 3.20 / 6.40 |
| ab-chan | 1 | 61 | 144 | 15431 | 208255 | 232319 | 24 | 3.20 / 12.80 |
| ab-chan | 1 | 60 | 123 | 1700 | 27471 | 47903 | 24 | 3.20 / 12.80 |
| ab-chan | 1 | 57 | 106 | 238 | 5483 | 21311 | 24 | 3.20 / 6.40 |
| ab-chan | 1 | 56 | 102 | 170 | 610 | 3115 | 24 | 3.20 / 6.40 |

ab-ring gateway process CPU seconds per run (~45 s each): 58 57 57 56 55 
ab-chan gateway process CPU seconds per run (~45 s each): 32 31 31 31 31 
