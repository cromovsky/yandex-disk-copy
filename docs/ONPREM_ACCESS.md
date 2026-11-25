<details class="help-section" id="onprem-access" markdown="1">
<summary>Доступ к SaaS и On-premises для переноса на личные и общие Диски</summary>

<details class="help-chapter" id="onprem-source-access" markdown="1">
<summary>Источник SaaS — приложение и права для чтения исходного Диска</summary>

Владелец организации SaaS один раз регистрирует рабочее приложение и сохраняет прежние приложения и их права при обновлении списка. Для чтения источника нужно `cloud_api:disk.read`, а для публикации файла или папки на личном Диске — ещё и `cloud_api:disk.write`. Общий Диск SaaS утилита читает с токеном сотрудника, у которого есть доступ к этому Диску. [Регистрация и проверка сервисного приложения SaaS](INSTRUCTION.md#saas-access) · [Публикация ресурса](https://yandex.ru/dev/disk-api/doc/ru/reference/publish)

| Поле формы | Что вводить |
|---|---|
| **ID организации (ORGID)** | ID под названием организации. Откройте «Общие настройки» → «Профиль организации» в [кабинете администратора](https://admin.yandex.ru/) |
| **Client ID сервисного приложения** | Client ID рабочего приложения из его карточки на [Яндекс OAuth](https://oauth.yandex.ru/). Оно должно быть зарегистрировано как сервисное в организации источника |
| **Client secret** | Секрет того же рабочего приложения. ID и секрет разных приложений не подходят |
| **Токен администратора (ADMIN_TOKEN, необязательно)** | Токен с правом `directory:read_users` позволяет убедиться, что сотрудник входит в организацию SaaS и его учётная запись активна. [Получить токен администратора](INSTRUCTION.md#saas-access-modes) |
| **Email владельца личного Диска SaaS** | Email активного сотрудника из раздела «Сотрудники». Не UID, не ID диска и не адрес администратора |
| **Исходный путь** | Укажите путь на Диске. Для всего личного Диска это `/`, для папки — `/Документы`, для файла — `/Документы/report.pdf` |

Токен сотрудника утилита получает и обновляет автоматически по Client ID,
Client secret и email. ORGID и ADMIN_TOKEN в запрос выдачи OAuth-токена не входят.
ORGID обязателен в форме. Истёкший ADMIN_TOKEN замените вручную.
Если ADMIN_TOKEN указан, перед выдачей **и обновлением** токена проверяются
принадлежность сотрудника организации ORGID и его активность. Без ADMIN_TOKEN
сервисное приложение также выдаёт токен. Если сотрудник заблокирован, разблокируйте его в админке и повторите проверку. Dry Run его статус не меняет.

SaaS ADMIN_TOKEN не заменяет ни Client secret источника, ни токен назначения.
После перезапуска введите секрет и использованные токены заново. План и журнал сохраняются.

</details>

<details class="help-chapter" id="onprem-personal-access" markdown="1">
<summary>On-premises — зарегистрировать DISK_MIGRATION, получить токен владельца и проверить доступ</summary>

Владелец организации On-premises регистрирует `DISK_MIGRATION` один раз. По шагам 1–4 подготовьте доступы для записи на личный или общий Диск. Шаги 5–8 показывают, как вручную проверить работу приложения на личном Диске.

| Поле формы | Значение |
|---|---|
| **Адрес Disk API On-premises** | `https://cloud-api.<домен>/v1/disk`. Адрес выдачи токенов `https://oauth.<домен>/token` утилита определит сама |
| **Client ID** и **Client secret сервисного приложения On-premises** | `DISK_MIGRATION_CLIENT_ID` и `DISK_MIGRATION_CLIENT_SECRET` из шага 3 |
| **Токен владельца организации On-premises** | Значение `access_token` из шага 2 |
| **Получатель On-premises** | Полный email или числовой UID сотрудника. Для списка укажите email в колонке `destination_email` каждой строки CSV |

Во время **Dry Run** утилита выпускает временный токен получателя и проверяет UID по сведениям о Диске. При дальнейшей работе утилита перевыпускает токен по мере необходимости. Для **общего Диска** укажите email оператора и метку Диска, куда требуется записать файлы. [Поля для общего Диска](#onprem-destination-access)

При возобновлении задания утилита сверяет сохранённый адрес OAuth с адресом `https://oauth.<домен>/token` для указанного Disk API. Если адреса расходятся, проверьте домен установки и составьте новый план. Старый план и его отчёт остаются в истории.

Если форма запрашивает ключ доступа к режиму On-premises, получите `ONPREM_ADMIN_TOKEN` у администратора утилиты. С подготовленными доступами переходите к переносу на личные Диски. [Один источник → личный Диск](ONPREM.md#onprem-personal) · [CSV → личные Диски](ONPREM.md#onprem-personal-list)

<details markdown="1">
<summary>1. Найти APIGW360_CLIENT_ID для получения токена владельца On-premises</summary>

Понадобятся `kubectl` с доступом на чтение секрета Passport, `jq`, `curl` и учётная запись владельца организации. Выполняйте примеры в одной сессии Bash на личном рабочем месте. Подставьте домен и идентификатор своей инсталляции. Функция извлекает только названное поле.

```bash
DOMAIN='office.example.org'
INSTANCE_ID='example'
set +x
set -o pipefail
passport_field() {
  kubectl -n "passport-${INSTANCE_ID}" get secret "passport-bootstrap-output-${INSTANCE_ID}" \
    -o "jsonpath={.data.$1}" | jq -Rer '@base64d | select(length > 0)'
}
APIGW360_CLIENT_ID=$(passport_field APIGW360_CLIENT_ID)
printf 'https://passport.%s/oauth/authorize?response_type=token&client_id=%s\n' \
  "$DOMAIN" "$APIGW360_CLIENT_ID"
```

</details>

<details markdown="1">
<summary>2. Получить OAuth-токен владельца организации On-premises</summary>

Откройте адрес из шага 1 в браузере. При первом входе или истёкшей авторизации войдите **как владелец организации On-premises** и подтвердите доступ. Скопируйте из адресной строки только значение между `access_token=` и ближайшим `&` (или до конца строки). Вставьте его в поле **«Токен владельца организации On-premises»**. Для команд ниже введите этот же токен скрыто в терминале и нажмите Enter.

```bash
read -rs OWNER_TOKEN
```

После закрытия страницы посмотреть токен на ней повторно нельзя. При необходимости пройдите авторизацию заново. Держите токены и секрет приложения вне исходного кода, журналов и истории команд.

</details>

<details markdown="1">
<summary>3. Найти Client ID и Client secret приложения DISK_MIGRATION</summary>

Используйте тот же секрет Kubernetes. Эти два значения понадобятся в форме и при ручном обмене токена. Команды сохраняют их в переменные текущей сессии.

```bash
DISK_MIGRATION_CLIENT_ID=$(passport_field DISK_MIGRATION_CLIENT_ID)
DISK_MIGRATION_CLIENT_SECRET=$(passport_field DISK_MIGRATION_CLIENT_SECRET)
```

Чтобы заполнить форму на macOS, скопируйте Client ID в локальный буфер и вставьте в поле **Client ID**. Затем повторите команду с переменной `$DISK_MIGRATION_CLIENT_SECRET` и вставьте значение в **Client secret**. В Linux с Wayland используйте `wl-copy` вместо `pbcopy`, с X11 — `xclip -selection clipboard`. Используйте буфер с выключенной историей.

```bash
printf '%s' "$DISK_MIGRATION_CLIENT_ID" | pbcopy
```

</details>

<details markdown="1">
<summary>4. Зарегистрировать DISK_MIGRATION и выдать права на чтение, запись и сведения о Диске</summary>

Для переноса на личный Диск нужны первые три права. Проверьте их у OAuth-клиента и в записи приложения в организации получателя.

| Право | Что разрешает |
|---|---|
| `cloud_api:disk.read` | Читать файлы и каталоги |
| `cloud_api:disk.write` | Создавать, изменять и удалять ресурсы |
| `cloud_api:disk.info` | Получать общие сведения о Диске, в том числе о его владельце |
| `cloud_api:disk.app_folder` | Работать в папке приложения. Это отдельный сценарий с доступом только внутри этой папки. Для миграции личного Диска это право не запрашивают |

ID организации On-premises указан в её карточке в модуле **«Управление поддержкой»** и в левом нижнем углу модуля **«Управление организацией»**. Подставьте его вместо примера. В отдельном рабочем каталоге получите текущий список и сохраните его в файл для редактирования.

```bash
ORG_ID='12345'
API="https://cloud-api.$DOMAIN"
APPS_URL="$API/v1/api360/security/org/$ORG_ID/service_applications"
curl -fsS -H "Authorization: OAuth $OWNER_TOKEN" "$APPS_URL" | \
  jq -e '.applications | arrays | {applications: .}' > applications.json
```

**POST заменяет весь список приложений организации.** Продолжайте только после успешного GET. Откройте `applications.json` и сохраните все существующие записи и права. В массив `applications` добавьте объект ниже, подставив фактический Client ID из шага 3. Если запись с таким ID уже есть, дополните её `scopes` недостающими правами.

```json
{
  "id": "<DISK_MIGRATION_CLIENT_ID>",
  "scopes": ["cloud_api:disk.read", "cloud_api:disk.write", "cloud_api:disk.info"]
}
```

Сверьте отредактированный массив с актуальным списком, в том числе с изменениями других администраторов. Отправьте **полный** файл и повторите GET. В ответе проверьте прежние приложения с их правами и регистрацию `DISK_MIGRATION` с тремя правами миграции. [Получить список](https://yandex.ru/dev/on-premises/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Get) · [Заменить список](https://yandex.ru/dev/on-premises/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Create)

```bash
curl -fsS -X POST "$APPS_URL" -H "Authorization: OAuth $OWNER_TOKEN" \
  -H 'Content-Type: application/json' --data-binary @applications.json
curl -fsS -H "Authorization: OAuth $OWNER_TOKEN" "$APPS_URL" | jq '.applications'
```

</details>

<details markdown="1">
<summary>5. Найти email или UID получателя в On-premises</summary>

Запросите список пользователей с токеном владельца и найдите нужного сотрудника по полному email. Команда выводит email и числовой UID из поля `id`. Для обмена токена подойдёт любое из этих значений. При постраничной выдаче проверьте следующие страницы каталога.

```bash
SUBJECT='employee@office.example.org'
curl -fsS "$API/v1/api360/directory/org/$ORG_ID/users" \
  -H "Authorization: OAuth $OWNER_TOKEN" | \
  jq -r --arg email "$SUBJECT" '.users[] | select(.email == $email) | [.email, .id] | @tsv'
```

</details>

<details markdown="1">
<summary>6. Получить временный токен получателя по email или UID вручную</summary>

Подставьте сотрудника из шага 5 и выберите тип его идентификатора. В форме утилиты тип определяется автоматически.

| `subject_token` | `subject_token_type` |
|---|---|
| Полный email сотрудника | `urn:yandex:params:oauth:token-type:email` |
| Числовой UID сотрудника | `urn:yandex:params:oauth:token-type:uid` |

```bash
SUBJECT='employee@office.example.org'
SUBJECT_TYPE='urn:yandex:params:oauth:token-type:email'
USER_TOKEN=$(curl --fail-with-body --silent --show-error -X POST "https://oauth.$DOMAIN/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange' \
  --data-urlencode "client_id=$DISK_MIGRATION_CLIENT_ID" \
  --data-urlencode "client_secret=$DISK_MIGRATION_CLIENT_SECRET" \
  --data-urlencode "subject_token=$SUBJECT" \
  --data-urlencode "subject_token_type=$SUBJECT_TYPE" \
  --data-urlencode 'scope=cloud_api:disk.read cloud_api:disk.write cloud_api:disk.info' \
  --data-urlencode "access_token=$OWNER_TOKEN" | \
  jq -er '.access_token // error(.error_description // .error // "Нет access_token")')
```

Поле `access_token` в ответе содержит временный токен **пользователя**. Он действует не более **3600 секунд**. `refresh_token` не выдаётся. При ручной работе повторите Token Exchange за 5–10 минут до истечения срока.

</details>

<details markdown="1">
<summary>7. Сверить UID владельца Диска и прочитать корневую папку</summary>

С временным токеном запросите сведения о Диске и сверьте `user.uid` с получателем. Второй GET должен вернуть корневую папку и её содержимое.

```bash
curl -fsS -H 'Accept: application/json' -H "Authorization: OAuth $USER_TOKEN" \
  "$API/v1/disk/"
curl -fsS --get "$API/v1/disk/resources" -H "Authorization: OAuth $USER_TOKEN" \
  --data-urlencode 'path=/'
```

</details>

<details markdown="1">
<summary>8. Загрузить тестовый файл на личный Диск получателя</summary>

Выберите локальный тестовый файл и новое имя в **существующей родительской папке** получателя. Первый запрос возвращает `href`, временный адрес для загрузки. Он действует **30 минут** и позволяет загрузить файл любому, кто его получил. Держите ссылку вне журналов и переписки.

```bash
LOCAL_FILE='./probe.txt'
DISK_FILE='/Документы/probe.txt'
UPLOAD=$(curl -fsS --get "$API/v1/disk/resources/upload" \
  -H "Authorization: OAuth $USER_TOKEN" \
  --data-urlencode "path=$DISK_FILE" --data-urlencode 'overwrite=false')
HREF=$(jq -er '.href | strings | select(startswith("https://"))' <<< "$UPLOAD")
OPERATION_ID=$(jq -r '.operation_id // empty | @uri' <<< "$UPLOAD")
curl -fsS -X PUT --upload-file "$LOCAL_FILE" --output /dev/null \
  --write-out 'HTTP %{http_code}\n' "$HREF"
```

Второй запрос отправляет файл прямо по `href`, **без заголовка OAuth**. `201 Created` означает, что файл создан. При `202 Accepted` сервер принял загрузку в обработку. Если вместе с `href` пришёл `operation_id`, проверьте статус операции с токеном сотрудника.

```bash
curl -fsS -H "Authorization: OAuth $USER_TOKEN" \
  "$API/v1/disk/operations/$OPERATION_ID" | jq -r '.status'
```

При `in-progress` повторите проверку статуса позже. `failed` означает ошибку загрузки, `success` — завершение. Проверьте итог по полному пути и сравните `size` с размером локального файла в байтах. Если сервер принял операцию без ID, результат можно проверить этим же запросом после появления файла.

```bash
curl -fsS --get "$API/v1/disk/resources" -H "Authorization: OAuth $USER_TOKEN" \
  --data-urlencode "path=$DISK_FILE" | jq '{path, size, md5}'
wc -c < "$LOCAL_FILE"
```

В ручном запросе `overwrite=false` защищает одноимённый файл. Укажите `overwrite=true` только для намеренной замены. При переносе на личный Диск утилита сама создаёт недостающие папки, пропускает совпадающий файл после проверки размера и MD5 и показывает конфликт при другом содержимом.

</details>

<details markdown="1">
<summary>Ошибки регистрации DISK_MIGRATION и выдачи токена получателя</summary>

| Сообщение API | Что проверить |
|---|---|
| `Wrong client secret` | Сверьте пару `DISK_MIGRATION_CLIENT_ID` и `DISK_MIGRATION_CLIENT_SECRET` из одного секрета инсталляции |
| `Scope from POST does not match the client's one` | Одно из запрошенных прав отсутствует у OAuth-клиента. Сверьте его права со значением `scope` в запросе |
| `No scopes allowed for token exchange` | У OAuth-клиента и записи приложения в организации нет общего разрешённого права. Проверьте обе настройки |
| `err_scope not empty` | Неизвестное имя права. В поле `scope` запроса Token Exchange используйте значения из шага 6. `yadisk:disk` в этом поле не поддерживается |
| `Unauthorized` | Проверьте, что указанный `client_id` зарегистрирован именно в организации выбранного сотрудника |
| `Subject token ... uid type must be a number` | Для типа `uid` нужен числовой UID. Для email выберите тип `urn:yandex:params:oauth:token-type:email` |

</details>

</details>

<details class="help-chapter" id="onprem-destination-access" markdown="1">
<summary>Общий Диск On-premises — указать сотрудника, метку Диска и доступы сервисного приложения</summary>

Выберите **«Общий диск»**. Для одного источника или всего списка CSV укажите сотрудника с правами `read` и `write` на выбранный общий Диск.

| Поле формы | Что вводить |
|---|---|
| **Email сотрудника с доступом к общему Диску** | Полный email сотрудника On-premises, под которым утилита будет записывать файлы |
| **Метка существующего общего диска** | Вставьте ссылку на корень общего Диска или саму метку. Например, для `https://disk.office.example/client/vd/team_42/disk` метка — `team_42`. [Информация об общем диске](https://yandex.ru/dev/on-premises/disk-api/doc/ru/reference/shared-disks/shd-info) |
| **Адрес Disk API On-premises** | `https://cloud-api.<домен>/v1/disk`. Утилита определит адрес OAuth по домену установки |
| **Client ID и Client secret** | Данные зарегистрированного сервисного приложения On-premises |
| **Токен владельца организации On-premises** | Токен для выдачи и обновления временного токена сотрудника |
| **Ключ доступа к режиму on-premises** | Если поле показано, получите ключ `ONPREM_ADMIN_TOKEN` у администратора утилиты |

Адрес API и данные приложения общие для всего списка. При Dry Run утилита получает временный токен по email, сохраняет UID сотрудника и проверяет доступ к общему Диску по его метке. При перевыпуске токена UID должен совпасть с сохранённым в плане. [Подготовка сервисного приложения](#onprem-personal-access)

Когда доступы готовы, переходите к переносу на выбранный общий Диск. [Один источник → общий Диск](ONPREM.md#onprem-shared) · [CSV → один общий Диск](ONPREM.md#onprem-shared-list)

<details markdown="1">
<summary>Как продолжить старое задание с токеном сотрудника</summary>

Откройте задание или список в истории. Если при создании использован готовый токен сотрудника, форма покажет поля **«Владелец токена назначения»** и **«Токен сотрудника с доступом к общему диску»**. Для продолжения введите токен и подтвердите его владельца. В старом одиночном задании кнопки «Составить новый план» и «Начать заново» сначала открывают форму сервисного приложения. Введите его доступы и нажмите «Проверить план».

1. Попросите администратора сообщить адрес **Passport** и `APIGW360_CLIENT_ID` установки. Этот Client ID нужен только для получения токена, в форму переноса он не вводится.
2. Откройте ссылку и войдите под оператором, у которого есть доступ к общему Диску:

```text
https://passport.onprem.example.org/oauth/authorize?response_type=token&client_id=<APIGW360_CLIENT_ID>
```

3. Скопируйте **значение `access_token`** из адресной строки. Вставляйте без `OAuth`, без `access_token=` и без остальной ссылки. [Получение OAuth-токенов](https://yandex.ru/dev/on-premises/disk-api/doc/ru/concepts/quickstart#oauth)
4. Укажите владельца этого токена и отметьте подтверждение под формой. Один оператор может переносить данные разных SaaS-сотрудников на общий Диск.

Для входа готовым токеном утилита сверяет владельца, если API возвращает `user.login` или `user.email`. Если API опускает эти поля, используется ваше подтверждение.

Администратор может получить `APIGW360_CLIENT_ID` из секрета Kubernetes `passport-bootstrap-output-<имя инсталляции>` в пространстве имён `passport-<имя инсталляции>`. Передайте оператору только Client ID. [Шаг 3 руководства по развёртыванию](https://yandex.ru/support/yandex-360/business/on-premises/deployment/ru/after-install/create-organization#get-data-token)

</details>

</details>

</details>
