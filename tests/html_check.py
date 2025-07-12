import os
import re
import sys
import importlib.util
import datetime
import requests
from unittest import mock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
PARSERS_DIR = os.path.join(PROJECT_ROOT, 'parsers')

URL_PATTERN = re.compile(r"(?:http|https|ftp)://[\w\-./?&=%]+")
SIMPLE_URL_PATTERN = re.compile(r"['\"]((?:http|https|ftp)://[^'\"\\s]+)['\"]")
F_STRING_PATTERN = re.compile(r"f(['\"])(.*?)\1", re.DOTALL)

FAKE_VALUES = {
    'station_iaga': 'TEST',
    'param': 'TEST',
    'start_date': '2025-07-01',
    'end_date': '2025-07-09',
    'sid': '123',
    'since_day': '01',
    'since_month': '07',
    'since_year': '2025',
    'until_day': '09',
    'until_month': '07',
    'until_year': '2025',
}

def substitute_fstring(fstring):
    def replacer(match):
        var_name = match.group(1)
        return FAKE_VALUES.get(var_name, 'TEST')
    return re.sub(r"\{(\w+)\}", replacer, fstring)

def extract_urls_from_file(filepath):
    urls = set()
    with open(filepath, encoding='utf-8') as f:
        content = f.read()

        urls.update(URL_PATTERN.findall(content))
        urls.update(SIMPLE_URL_PATTERN.findall(content))

        fstrings = F_STRING_PATTERN.findall(content)
        for _, fstr in fstrings:
            substituted = substitute_fstring(fstr)
            urls.update(URL_PATTERN.findall(substituted))

    # Исключаем ссылки с 'proxy' внутри
    urls = {url for url in urls if 'proxy' not in url.lower()}

    return urls

def check_url(url):
    if 'proxy' in url.lower():
        return None, "Пропущена ссылка с proxy"

    try:
        if re.match(r"^(http|https|ftp)://(localhost|127\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+|172\.(1[6-9]|2\d|3[0-1])\.\d+\.\d+)", url):
            return None, "Пропущена локальная ссылка"

        if url.startswith('ftp://'):
            import ftplib
            from urllib.parse import urlparse
            parsed = urlparse(url)
            ftp = ftplib.FTP()
            ftp.connect(parsed.hostname, parsed.port or 21, timeout=10)
            ftp.quit()
            return True, "FTP OK"
        else:
            resp = requests.get(url, timeout=10)
            return resp.ok, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)

def load_parser_module(parser_path):
    with mock.patch.dict(sys.modules, {
        'bs4': mock.MagicMock(),
        'psycopg2.extras': mock.MagicMock(),
        'psycopg2': mock.MagicMock(),
        'modules': mock.MagicMock(),
        'modules.postgres': mock.MagicMock(),
        'modules.redis': mock.MagicMock(),
        'modules.logger_setup': mock.MagicMock(),
        'modules.utils': mock.MagicMock(),
        'modules.proxy': mock.MagicMock(),
    }):
        spec = importlib.util.spec_from_file_location("parser_module", parser_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules["parser_module"] = module
        spec.loader.exec_module(module)
        return module

def main():
    report = []
    if not os.path.exists(PARSERS_DIR):
        print(f"Папка с парсерами не найдена: {PARSERS_DIR}")
        return

    print("Начинаем анализ парсеров и проверку ссылок...")

    for parser_name in sorted(os.listdir(PARSERS_DIR)):
        parser_path = os.path.join(PARSERS_DIR, parser_name)
        if not os.path.isdir(parser_path):
            continue

        parser_py = os.path.join(parser_path, 'parser.py')
        urls_to_check = set()

        if os.path.exists(parser_py):
            try:
                mod = load_parser_module(parser_py)

                if hasattr(mod, 'build_url') and callable(mod.build_url):
                    try:
                        test_since_date = datetime.date(2023, 1, 1)
                        test_until_date = datetime.date(2023, 1, 2)
                        url = mod.build_url(test_since_date, test_until_date)
                        if url:
                            urls_to_check.add(url)
                    except Exception:
                        pass

                if hasattr(mod, 'BASE_URL_TEMPLATE'):
                    template = mod.BASE_URL_TEMPLATE
                    try:
                        url = template.format(**FAKE_VALUES)
                        if url:
                            urls_to_check.add(url)
                    except Exception:
                        pass

                if hasattr(mod, 'BASE_URL') and isinstance(mod.BASE_URL, str) and mod.BASE_URL.startswith(('http', 'ftp')):
                    if 'BASE_URL_TEMPLATE' not in mod.__dict__ and 'build_url' not in mod.__dict__:
                        urls_to_check.add(mod.BASE_URL)

            except Exception:
                if "parser_module" in sys.modules:
                    del sys.modules["parser_module"]

        for root, _, files in os.walk(parser_path):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    extracted_urls = extract_urls_from_file(file_path)
                    urls_to_check.update(extracted_urls)

        # Группируем ссылки по базовому URL (до '?')
        base_url_map = {}
        for url in urls_to_check:
            base_url = url.split('?', 1)[0]
            base_url_map.setdefault(base_url, []).append(url)

        # Фильтруем: если есть сборочная (с параметрами) — простую не добавляем
        filtered_urls = set()
        for base_url, url_list in base_url_map.items():
            has_full = any('?' in u for u in url_list)
            if has_full:
                filtered_urls.update(u for u in url_list if '?' in u)
            else:
                filtered_urls.update(url_list)

        for url in sorted(filtered_urls):
            ok, msg = check_url(url)
            if ok is None:
                continue
            report.append({
                'parser': parser_name,
                'file': os.path.basename(parser_py),
                'url': url,
                'result': 'OK' if ok else 'FAIL',
                'info': msg
            })

    # Итоговый отчёт
    print("Отчёт о проверке ссылок:")
    for idx, row in enumerate(report, 1):
        print(f"\n{idx}. Парсер: {row['parser']}")
        print(f"   Ссылка: {row['url']}")
        print(f"   Статус: {row['result']}")
        print(f"   Инфо: {row['info']}")

    print(f"\nИтого проверено: {len(report)} ссылок")

if __name__ == '__main__':
    main()
