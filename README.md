# 1t-attestation-1.2

## Пояснения к решению

Все таблицы создаются в **init.sql**:

### 1. Staging слой

Содержит таблицу **st_vacancy**, в которую попадают все очищенные данные из исходного файла

### 2. Core слой

Содержит таблицу **cr_vacancy**, в которой содержатся только нужные поля, без полей (professional_roles, specializations, profarea_names)

### 3. Datamart слой

Содержит три витрины:
* **dm_vacancy_msc_slr**, которая содержит вакансии по городу Москве, в которых указана заработная плата
* **dm_vacancy_spb_slr**, которая содержит вакансии по городу Санкт-Петербург, в которых указана заработная плата
* **dm_salary_avg**, содержит средние минимальные и максимальные заработные платы по Москве и Санкт-Петербургу, с указанием города, в котором вакансия более распространена.

В витрине dm_salary_avg введены дополнительные поля:
* **msc_from_avg**  - Средняя минимальная заработная плата, по текущей должности, в Москве
* **msc_to_avg**    - Средняя максимальная заработная плата, по текущей должности, в Москве
* **spb_from_avg**  - Средняя минимальная заработная плата, по текущей должности, в Санкт-Петербурге
* **spb_to_avg**    - Средняя максимальная заработная плата, по текущей должности, в Санкт-Петербурге
* **most_common**   - Город, в котором должность наиболее распространена

### Количество вакансий после удаления:
* В Москве - 11 210 вакансий (с указанием зп)
* В Санкт-Петербурге - 6 007 вакансий (с указанием зп)