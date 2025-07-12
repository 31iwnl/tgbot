#!/usr/bin/env python

"""
Fixes a MySQL dump made with the right format so it can be directly
imported to a new PostgreSQL database.

Dump using:
mysqldump --compatible=postgresql --default-character-set=utf8 -r databasename.mysql -u root databasename
"""

import re
import sys
import os
import time


def parse(input_filename, output_filename):
    "Feed it a file, and it'll output a fixed one"

    # Подсчёт строк без использования wc (кроссплатформенно)
    if input_filename == "-":
        num_lines = -1
    else:
        def count_lines(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        num_lines = count_lines(input_filename)

    tables = {}
    current_table = None
    creation_lines = []
    enum_types = []
    foreign_key_lines = []
    fulltext_key_lines = []
    sequence_lines = []
    cast_lines = []
    num_inserts = 0
    started = time.time()

    # Открываем файлы
    if output_filename == "-":
        output = sys.stdout
        logging = open(os.devnull, "w")
    else:
        output = open(output_filename, "w", encoding='utf-8')
        logging = sys.stdout

    if input_filename == "-":
        input_fh = sys.stdin
    else:
        input_fh = open(input_filename, encoding='utf-8')

    output.write("-- Converted by db_converter\n")
    output.write("START TRANSACTION;\n")
    output.write("SET standard_conforming_strings=off;\n")
    output.write("SET escape_string_warning=off;\n")
    output.write("SET CONSTRAINTS ALL DEFERRED;\n\n")

    for i, line in enumerate(input_fh):
        time_taken = time.time() - started
        if num_lines > 0:
            percentage_done = (i+1) / float(num_lines)
            secs_left = (time_taken / percentage_done) - time_taken
        else:
            percentage_done = 0
            secs_left = 0
        logging.write("\rLine %i (of %s: %.2f%%) [%s tables] [%s inserts] [ETA: %i min %i sec]" % (
            i + 1,
            num_lines if num_lines > 0 else "?",
            percentage_done*100,
            len(tables),
            num_inserts,
            int(secs_left // 60),
            int(secs_left % 60),
        ))
        logging.flush()

        # В Python 3 line уже str, decode не нужен
        line = line.strip().replace(r"\\", "WUBWUBREALSLASHWUB").replace(r"\'", "''").replace("WUBWUBREALSLASHWUB", r"\\")

        # Игнорируем комментарии и служебные строки
        if line.startswith("--") or line.startswith("/*") or line.startswith("LOCK TABLES") or line.startswith("DROP TABLE") or line.startswith("UNLOCK TABLES") or not line:
            continue

        # Обработка состояния
        if current_table is None:
            if line.startswith("CREATE TABLE"):
                # Получаем имя таблицы из кавычек
                # В MySQL дампе имя может быть в обратных кавычках ` или двойных "
                # Попробуем универсально
                match = re.search(r'CREATE TABLE [`"]?(\w+)[`"]?', line)
                if match:
                    current_table = match.group(1)
                    tables[current_table] = {"columns": []}
                    creation_lines = []
                else:
                    print(f"\n ! Не удалось определить имя таблицы в строке: {line}")
            elif line.startswith("INSERT INTO"):
                # Заменяем '0000-00-00 00:00:00' на NULL
                fixed_line = line.replace("'0000-00-00 00:00:00'", "NULL")
                output.write(fixed_line + "\n")
                num_inserts += 1
            else:
                # Можно убрать или оставить для отладки
                # print(f"\n ! Unknown line in main body: {line}")
                pass

        else:
            # Внутри CREATE TABLE
            if line.startswith('"') or line.startswith('`'):
                # Разбор колонки
                # Убираем начальные и конечные кавычки, разделяем по пробелу
                parts = re.split(r'"|`', line.strip(","))
                if len(parts) >= 3:
                    name = parts[1]
                    definition = parts[2].strip()
                else:
                    print(f"\n ! Не удалось разобрать определение колонки: {line}")
                    continue

                try:
                    type_part, extra_part = definition.split(" ", 1)
                except ValueError:
                    type_part = definition
                    extra_part = ""

                extra_part = re.sub(r"CHARACTER SET [\w\d]+\s*", "", extra_part.replace("unsigned", ""))
                extra_part = re.sub(r"COLLATE [\w\d]+\s*", "", extra_part.replace("unsigned", ""))

                # Преобразование типов
                final_type = None
                set_sequence = None
                type_lower = type_part.lower()
                if type_lower.startswith("tinyint("):
                    type_part = "int4"
                    set_sequence = True
                    final_type = "boolean"
                elif type_lower.startswith("int("):
                    type_part = "integer"
                    set_sequence = True
                elif type_lower.startswith("bigint("):
                    type_part = "bigint"
                    set_sequence = True
                elif type_lower == "longtext":
                    type_part = "text"
                elif type_lower == "mediumtext":
                    type_part = "text"
                elif type_lower == "tinytext":
                    type_part = "text"
                elif type_lower.startswith("varchar("):
                    size = int(re.search(r'\((\d+)\)', type_part).group(1))
                    type_part = f"varchar({size * 2})"
                elif type_lower.startswith("smallint("):
                    type_part = "int2"
                    set_sequence = True
                elif type_lower == "datetime":
                    type_part = "timestamp with time zone"
                elif type_lower == "double":
                    type_part = "double precision"
                elif type_lower.endswith("blob"):
                    type_part = "bytea"
                elif type_lower.startswith("enum(") or type_lower.startswith("set("):
                    types_str = type_part.split("(",1)[1].rstrip(")").rstrip('"')
                    types_arr = [t.strip("'") for t in types_str.split(",")]
                    enum_name = f"{current_table}_{name}"
                    if enum_name not in enum_types:
                        output.write(f"CREATE TYPE {enum_name} AS ENUM ({types_str}); \n")
                        enum_types.append(enum_name)
                    type_part = enum_name

                if final_type:
                    cast_lines.append(f"ALTER TABLE \"{current_table}\" ALTER COLUMN \"{name}\" DROP DEFAULT, ALTER COLUMN \"{name}\" TYPE {final_type} USING CAST(\"{name}\" as {final_type})")
                if name == "id" and set_sequence:
                    sequence_lines.append(f"CREATE SEQUENCE {current_table}_id_seq")
                    sequence_lines.append(f"SELECT setval('{current_table}_id_seq', max(id)) FROM {current_table}")
                    sequence_lines.append(f"ALTER TABLE \"{current_table}\" ALTER COLUMN \"id\" SET DEFAULT nextval('{current_table}_id_seq')")

                creation_lines.append(f'"{name}" {type_part} {extra_part}')
                tables[current_table]['columns'].append((name, type_part, extra_part))

            elif line.startswith("PRIMARY KEY"):
                creation_lines.append(line.rstrip(","))
            elif line.startswith("CONSTRAINT"):
                foreign_key_lines.append(f"ALTER TABLE \"{current_table}\" ADD CONSTRAINT {line.split('CONSTRAINT')[1].strip().rstrip(',')} DEFERRABLE INITIALLY DEFERRED")
                foreign_key_lines.append(f"CREATE INDEX ON \"{current_table}\" {line.split('FOREIGN KEY')[1].split('REFERENCES')[0].strip().rstrip(',')}")
            elif line.startswith("UNIQUE KEY"):
                creation_lines.append(f"UNIQUE ({line.split('(')[1].split(')')[0]})")
            elif line.startswith("FULLTEXT KEY"):
                fulltext_keys = " || ' ' || ".join(line.split('(')[-1].split(')')[0].replace('"', '').split(','))
                fulltext_key_lines.append(f"CREATE INDEX ON {current_table} USING gin(to_tsvector('english', {fulltext_keys}))")
            elif line.startswith("KEY"):
                pass
            elif line == ");":
                output.write(f"CREATE TABLE \"{current_table}\" (\n")
                for idx, l in enumerate(creation_lines):
                    output.write(f"    {l}{',' if idx != len(creation_lines) - 1 else ''}\n")
                output.write(");\n\n")
                current_table = None
            else:
                print(f"\n ! Unknown line inside table creation: {line}")

    # Завершение файла
    output.write("\n-- Post-data save --\n")
    output.write("COMMIT;\n")
    output.write("START TRANSACTION;\n")

    output.write("\n-- Typecasts --\n")
    for line in cast_lines:
        output.write(f"{line};\n")

    output.write("\n-- Foreign keys --\n")
    for line in foreign_key_lines:
        output.write(f"{line};\n")

    output.write("\n-- Sequences --\n")
    for line in sequence_lines:
        output.write(f"{line};\n")

    output.write("\n-- Full Text keys --\n")
    for line in fulltext_key_lines:
        output.write(f"{line};\n")

    output.write("\nCOMMIT;\n")
    print("Conversion complete.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python tes.py input_mysql_dump.sql output_postgres_dump.sql")
        sys.exit(1)
    parse(sys.argv[1], sys.argv[2])
