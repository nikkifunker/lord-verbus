# Lord Verbus Bot

## Добавление ачивок

Используйте команду `/ach_add` в формате `code|title|description|kind|source|data`.

Поддерживаемые варианты:

1. `/ach_add code|title|description|tiered|messages|100,1000,10000`
2. `/ach_add code|title|description|single|messages|100`
3. `/ach_add code|title|description|single|date|YYYY-MM-DD`
4. `/ach_add code|title|description|tiered|keyword:WORD|1,3,5`
5. `/ach_add code|title|description|tiered|voice|10,50,100`
6. `/ach_add code|title|description|single|voice|25`
7. `/ach_add code|title|description|tiered|videonote|5,20,50`
8. `/ach_add code|title|description|tiered|sticker|50,200,500`

Для `tiered` передавайте список порогов через запятую. Для `single` укажите ровно одно число. Для `keyword` используйте формат `keyword:WORD`.
