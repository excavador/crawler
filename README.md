# crawler

example of usage:
```
./scripts/venv ./crawler.py --parallel=16 http://yandex.ru/
./scripts/venv ./crawler.py --parallel=16 http://google.ru/
./scripts/venv ./crawler.py --parallel=16 http://htmlbook.ru/
```

# потраченное время

4 часа 15 минут

# постановка задачи

Задача:
   Реализовать web-crawler, рекурсивно скачивающий сайт (идущий по ссылкам вглубь). Crawler должен скачать документ по указанному URL и продолжить закачку по ссылкам, находящимся в документе.
   Crawler должен поддерживать дозакачку.
   Crawler должен грузить только текстовые документы -   html, css, js (игнорировать картинки, видео, и пр.)
   Crawler должен грузить документы только одного домена (игнорировать сторонние ссылки)
   Crawler должен быть многопоточным (какие именно части параллелить - полностью ваше решение)

Требования специально даны неформально. Мы ходим увидеть, как вы по постановке задаче самостоятельно примете решение, что более важно, а что менее.

На выходе мы ожидаем работающее приложение, которое сможем собрать и запустить.
Мы не ожидаем правильной обработки всех типов ошибок и граничных случаев, вы сами себе должны поставить отсечку "good enough".
