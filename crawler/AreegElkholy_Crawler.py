
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import date
from datetime import datetime
import mysql.connector

#FUNCTIONS TO INSERT INTO EVRY TABLE IN MY DATABASE

def insert_iteration(cursor, iteration_number, year):
    sql = """
    INSERT IGNORE INTO Iteration (Number, Year)
    VALUES (%s, %s)
    """
    cursor.execute(sql, (iteration_number, year))

def insert_iteration_host(cursor, iteration_number, first_name, middle_name, last_name):
    sql = """
    INSERT IGNORE INTO IterationHost (IterationNumber, HostFirstName, HostMiddleName, HostLastName)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(sql, (iteration_number, first_name, middle_name, last_name))


def insert_movie(cursor, name, release_date, budget=None, box_office=None, runtime=None, language=None):
    sql = """
    INSERT IGNORE INTO Movie (Name, ReleaseDate, Budget, BoxOffice, Runtime, Language)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (name, release_date, budget, box_office, runtime, language))

def insert_production_company(cursor, company_name):
    sql = """
    INSERT IGNORE INTO ProductionCompany (ProductionCompanyName)
    VALUES (%s)
    """
    cursor.execute(sql, (company_name,))

def insert_movie_production_company(cursor, movie_name, movie_release_date, company_name):
    sql = """
    INSERT IGNORE INTO MovieProductionCompany (MovieName, MovieReleaseDate, ProductionCompanyName)
    VALUES (%s, %s, %s)
    """
    cursor.execute(sql, (movie_name, movie_release_date, company_name))

def insert_person(cursor, first_name, middle_name, last_name, birth_date, country, death_date):
    sql = """
    INSERT IGNORE INTO People (FirstName, MiddleName, LastName, BirthDate, Country, DeathDate)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (first_name, middle_name, last_name, birth_date, country, death_date))

def insert_nomination_category(cursor, category_name):
    sql = """
    INSERT IGNORE INTO NominationCategory (CategoryName)
    VALUES (%s)
    """
    cursor.execute(sql, (category_name,))

def insert_nomination(cursor, iteration_number, movie_name, movie_release_date, category_name, 
                       p_firstname, p_lastname, p_birthdate, granted):
    sql = """
    INSERT IGNORE INTO Nomination
    (IterationNumber, MovieName, MovieReleaseDate, CategoryName,
     PersonFirstName, PersonLastName, PersonBirthDate, Granted)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (iteration_number, movie_name, movie_release_date, category_name, 
                         p_firstname, p_lastname, p_birthdate, granted))

def insert_role_in_movie(cursor, role, movie_name, movie_release_date, p_firstname, p_lastname, p_birthdate):
    sql = """
    INSERT IGNORE INTO RoleInMovie
    (Role, MovieName, MovieReleaseDate, PersonFirstName, PersonLastName, PersonBirthDate)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (role, movie_name, movie_release_date, p_firstname, p_lastname, p_birthdate))



#CONNECTING TO MY DB
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='areeg2005', 
        database='AcademyAwards'
    )


#HELPER FUNCTIONS THAT ARE NEEDED THROUHGOHUT MY CODE
def get_best_guess_wikipedia_url(name):
    try:
        print(f"Searching Wikipedia for: {name}")
        response = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "list": "search",
                "srsearch": name,
                "format": "json"
            },
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=5
        )
        data = response.json()
        search_results = data.get("query", {}).get("search", [])
        if search_results:
            title = search_results[0]["title"]
            return "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
    except Exception as e:
        print(f"Search failed for {name}: {e}")
    return None


def clean_text_with_refs(tag):
    if tag:
        for sup in tag.find_all("sup"):
            sup.decompose()
        return tag.get_text(" ", strip=True)
    return ""


def parse_name(full_name):
    parts = full_name.split()
    if not parts:
        return (None, None, None)
    first_name = parts[0]
    if len(parts) == 1:
        return (first_name, None, None)
    elif len(parts) == 2:
        return (first_name, None, parts[1])
    else:
        last_name = parts[-1]
        middle_name = " ".join(parts[1:-1])
        return (first_name, middle_name, last_name)

def get_iteration(i):
    if 10 <= i % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(i % 10, 'th')
    return f"{i}{suffix}"


def parse_money_to_decimal(money_str):
    if not money_str:
        return None

    money_str = money_str.lower().replace(",", "").strip()
    money_str = money_str.replace("$", "")

    factor = 1
    if "million" in money_str:
        factor = 1_000_000
        money_str = money_str.replace("million", "")
    elif "billion" in money_str:
        factor = 1_000_000_000
        money_str = money_str.replace("billion", "")

    match = re.search(r'(\d+(\.\d+)?)', money_str)
    if match:
        try:
            val = float(match.group(1)) * factor
            return val
        except ValueError:
            pass

    return None

def parse_date_from_string(text):
    if not text:
        return None

    text = re.sub(r'\[\d+\]', '', text)  
    text = text.strip().strip('"').strip()

   
    match = re.search(r'([A-Za-z]+\s+\d{1,2},\s*\d{4})', text)
    if match:
        try:
            dt = datetime.strptime(match.group(1), "%B %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    match = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', text)
    if match:
        try:
            dt = datetime.strptime(match.group(1), "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    match = re.search(r'\b(19|20)\d{2}\b', text)
    if match:
        return f"{match.group(0)}-01-01"

    return None

#GETTING INFO TO INSERT INTO THE PEOPLE TABLE
def get_person_details(person_url):
    try:
        response = requests.get(
            person_url,
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=10
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        result = {
            "birth_date": None,
            "country": None,
            "death_date": None
        }

        infobox = soup.find("table", class_=lambda x: x and "infobox" in x)
        if infobox:
            for row in infobox.find_all("tr"):
                th = row.find("th")
                if not th:
                    continue

                label = th.get_text(" ", strip=True).lower()
                td = row.find("td")
                if not td:
                    continue

                for tag in td.find_all("sup"):
                    tag.decompose()

                visible_strings = [s for s in td.strings if s.strip()]
                raw_text = " ".join(visible_strings)

                if "born" in label:
                    bday_span = td.select_one("span.bday")
                    if bday_span:
                        result["birth_date"] = bday_span.get_text(strip=True)
                    else:
                        for text_part in visible_strings:
                            for fmt in ("%d %B %Y", "%B %d, %Y"):
                                try:
                                    dt = datetime.strptime(text_part.strip(), fmt)
                                    result["birth_date"] = dt.strftime("%Y-%m-%d")
                                    break
                                except ValueError:
                                    continue
                            if result["birth_date"]:
                                break

                        if not result["birth_date"]:
                            year_match = re.search(r'\b(18|19|20)\d{2}\b', raw_text)
                            if year_match:
                                result["birth_date"] = f"{year_match.group(0)}-01-01"

                    birthplace_div = td.find(class_="birthplace")
                    if not birthplace_div:
                        for div in td.find_all("div", style="display:inline"):
                            class_list = div.get("class", [])
                            if not any(c in ["nickname", "deathplace"] for c in class_list):
                                birthplace_div = div
                                break

                    if birthplace_div:
                        text = birthplace_div.get_text(" ", strip=True)
                        if "," in text:
                            result["country"] = re.sub(r"[()]", "", text.split(",")[-1].strip())
                        else:
                            result["country"] = re.sub(r"[()]", "", text.strip())

                elif "died" in label:
                    hidden_spans = td.find_all("span", style=lambda x: x and "display:none" in x)
                    for span in hidden_spans:
                        text = span.get_text(strip=True).strip("()")
                        if re.match(r"\d{4}-\d{2}-\d{2}", text):
                            result["death_date"] = text
                            break

                    if not result["death_date"]:
                        for text_part in visible_strings:
                            for fmt in ("%d %B %Y", "%B %d, %Y"):
                                try:
                                    dt = datetime.strptime(text_part.strip(), fmt)
                                    result["death_date"] = dt.strftime("%Y-%m-%d")
                                    break
                                except ValueError:
                                    continue
                            if result["death_date"]:
                                break

                        if not result["death_date"]:
                            year_match = re.search(r'\b(18|19|20)\d{2}\b', raw_text)
                            if year_match:
                                result["death_date"] = f"{year_match.group(0)}-01-01"

        return result

    except Exception as e:
        print(f"Error fetching person details from {person_url}: {str(e)}")
        return {
            "birth_date": None,
            "country": None,
            "death_date": None
        }


#TO CHECK IF THE PERSON ALREADY HAS DATA
def insert_or_update_person_details(cursor, first_name, middle_name, last_name, person_url):
    cursor.execute("""
        SELECT 1 FROM People 
        WHERE FirstName = %s AND LastName = %s
        AND (BirthDate IS NOT NULL OR Country IS NOT NULL OR DeathDate IS NOT NULL)
        LIMIT 1
    """, (first_name, last_name))
    
    if cursor.fetchone():
        return

    if not person_url:
        full_name = " ".join(filter(None, [first_name, middle_name, last_name]))
        person_url = get_best_guess_wikipedia_url(full_name)

    details = get_person_details(person_url)
    if not details or all(val is None for val in details.values()):
        full_name = " ".join(filter(None, [first_name, middle_name, last_name]))
        person_url = get_best_guess_wikipedia_url(full_name)
        details = get_person_details(person_url) if person_url else {}

    birth_date = details.get("birth_date")
    country = details.get("country")
    death_date = details.get("death_date")

    print(f"Inserting person: {first_name} {middle_name or ''} {last_name}, "
          f"Birth: {birth_date}, Country: {country}, Death: {death_date}")

    try:
        insert_person(
            cursor,
            first_name,
            middle_name,
            last_name,
            birth_date,
            country,
            death_date
        )
    except mysql.connector.Error as err:
        print(f"Error inserting person {first_name} {last_name}: {err}")



def get_production_companies(movie_soup):
    production_companies = []
    
    def clean_text_with_refs(tag):
        if tag:
            for sup in tag.find_all("sup"):
                sup.decompose()
            return tag.get_text(" ", strip=True)
        return ""

    infobox = movie_soup.find("table", class_=lambda x: x and "infobox" in x)
    if not infobox:
        return production_companies

    rows = infobox.find_all("tr")
    for row in rows:
        header = row.find("th", class_="infobox-label")
        data_cell = row.find("td", class_="infobox-data")
        if not header or not data_cell:
            continue

        label_text = header.get_text(" ", strip=True).lower()
        if "production" in label_text:
            li_tags = data_cell.find_all("li")
            if li_tags:
                for li in li_tags:
                    company_name = clean_text_with_refs(li)
                    if company_name:
                        production_companies.append(company_name)
            else:
                raw_text = clean_text_with_refs(data_cell)
                candidates = re.split(r'[\n,•]+', raw_text)
                for c in candidates:
                    company_name = c.strip()
                    if company_name:
                        production_companies.append(company_name)
    print("Extracted production companies:", production_companies)
    return production_companies

def extract_names_from_td(data_cell):
    for sup in data_cell.find_all("sup"):
        sup.decompose()

    names = []
    lines = list(data_cell.stripped_strings)

    for name in lines:
        name = name.strip().strip('"').strip()
        if name and len(name) > 1 and not re.fullmatch(r'\[?\s*[a-zA-Z0-9]+\s*\]?', name):
            names.append((name, None))  

    return names


def get_movie_roles(movie_soup):
    roles_dict = {
        "Director": [],
        "Producer": [],
        "Actor": []
    }
    infobox = movie_soup.find("table", class_=lambda x: x and "infobox" in x)
    if not infobox:
        return roles_dict

    rows = infobox.find_all("tr")
    for row in rows:
        header = row.find("th", class_="infobox-label")
        data_cell = row.find("td", class_="infobox-data")
        if not header or not data_cell:
            continue

        for sup in data_cell.find_all("sup"):
            sup.decompose()

        label_text = header.get_text(" ", strip=True).lower()

        if label_text in ["directed by", "produced by", "starring"]:
            role_key = (
                "Director" if "directed" in label_text
                else "Producer" if "produced" in label_text
                else "Actor"
            )
            extracted = []

            li_tags = data_cell.find_all("li")
            if li_tags:
                for li in li_tags:
                    for sup in li.find_all("sup"):
                        sup.decompose()

                    text = li.get_text(strip=True).lower()
                    if any(skip in text for skip in ['uncredited', 'soldiers', 'as', 'generals']):
                        continue

                    a_tag = li.find("a", href=True)
                    if a_tag:
                        raw_name = a_tag.get_text(strip=True)
                        person_url = "https://en.wikipedia.org" + a_tag["href"]
                        if raw_name and not re.fullmatch(r'\[?\s*[a-zA-Z0-9]+\s*\]?', raw_name):
                            extracted.append((raw_name, person_url))
                    else:
                        raw_name = li.get_text(strip=True)
                        if raw_name and not re.fullmatch(r'\[?\s*[a-zA-Z0-9]+\s*\]?', raw_name):
                            extracted.append((raw_name, None))
            else:
                a_tags = data_cell.find_all("a", href=True)
                for a_tag in a_tags:
                    raw_name = a_tag.get_text(strip=True)
                    if any(skip in raw_name.lower() for skip in ['uncredited', 'soldiers', 'as', 'generals']):
                        continue
                    person_url = "https://en.wikipedia.org" + a_tag["href"]
                    if raw_name and not re.fullmatch(r'\[?\s*[a-zA-Z0-9]+\s*\]?', raw_name):
                        extracted.append((raw_name, person_url))

                if not any(name for name, _ in extracted if name and not re.fullmatch(r'\[?\s*[a-zA-Z0-9]+\s*\]?', name)):
                    raw_text = data_cell.get_text(separator="|", strip=True)
                    for name in raw_text.split("|"):
                        clean_name = name.strip().strip('"')
                        if any(skip in clean_name.lower() for skip in ['uncredited', 'soldiers', 'as', 'generals']):
                            continue
                        if clean_name and not re.fullmatch(r'\[?\s*[a-zA-Z0-9]+\s*\]?', clean_name):
                            extracted.append((clean_name, None))

            roles_dict[role_key].extend(extracted)

    print("Extracted movie roles:", roles_dict)
    return roles_dict


def get_movie_details(movie_url):
    response = requests.get(movie_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    infobox = soup.find("table", class_=lambda x: x and "infobox" in x)
    if not infobox:
        print(f"No infobox found for movie: {movie_url}")
        return {
            "name": None,
            "release_date": None,
            "budget": None,
            "box_office": None,
            "runtime": None,
            "language": None,
            "soup": soup
        }

    name = None
    title_th = infobox.find("th", class_="infobox-above")
    if title_th:
        name = title_th.get_text(" ", strip=True)
    else:
        title_th_alt = infobox.find("th", attrs={"colspan": "2"})
        if title_th_alt:
            name = title_th_alt.get_text(" ", strip=True)

    release_date = None
    budget = None
    box_office = None
    runtime = None
    language = None

    rows = infobox.find_all("tr")
    for row in rows:
        header = row.find("th", class_="infobox-label")
        data_cell = row.find("td", class_="infobox-data")
        if not header or not data_cell:
            continue

        label = header.get_text(" ", strip=True).lower()

        if "release date" in label:
            dates_ul = data_cell.find("ul")
            if dates_ul:
                first_li = dates_ul.find("li")
                if first_li:
                    date_text = first_li.get_text(" ", strip=True)
                    parsed = parse_date_from_string(date_text)
                    if parsed:
                        release_date = parsed
            else:
                date_text = data_cell.get_text(" ", strip=True)
                parsed = parse_date_from_string(date_text)
                if parsed:
                    release_date = parsed

        elif "running time" in label:
            rt_text = data_cell.get_text(" ", strip=True)
            m = re.search(r'(\d+)\s*(?:min|mins|minutes)', rt_text.lower())
            if m:
                runtime = int(m.group(1))

        elif "budget" in label:
            raw_budget = data_cell.get_text(" ", strip=True)
            budget = parse_money_to_decimal(raw_budget)

        elif "box office" in label:
            raw_box_office = data_cell.get_text(" ", strip=True)
            box_office = parse_money_to_decimal(raw_box_office)

        elif "language" in label:
            text = data_cell.get_text(", ", strip=True)
            language = text.split(",", 1)[0].strip()

    movie_details = {
        "name": name,
        "release_date": release_date,
        "budget": budget,
        "box_office": box_office,
        "runtime": runtime,
        "language": language,
        "soup": soup
    }
    print(f"Extracted movie details for {movie_url}: {movie_details}")
    return movie_details


def extract_person_and_movies_from_li(li):
    for tag in li.find_all("sup"):
        tag.decompose()

    person_tag = None
    for a in li.find_all("a", href=True):
        if not a.find_parent("i"):
            person_tag = a
            break

    if not person_tag:
        return []

    person_name = person_tag.get_text(strip=True)
    person_url = "https://en.wikipedia.org" + person_tag["href"]

    pairs = []
    for i_tag in li.find_all("i"):
        movie_link = i_tag.find("a", href=True)
        if movie_link:
            movie_name = movie_link.get_text(strip=True)
            movie_url = "https://en.wikipedia.org" + movie_link["href"]
            pairs.append((person_name, person_url, movie_name, movie_url))

    return pairs



def extract_infobox_people(td_tag):
    people = []
    for a in td_tag.find_all("a", href=True):
        name = a.get_text(strip=True)
        href = a["href"]
        if name in {",", ".", "-", "–", "•"} or "List_of_" in href or "Category:" in href:
            continue
        people.append(name)
    return people


def get_nominations(soup):
    nominations = []

    person_categories = {
        "Best Directing", "Best Actor", "Best Director", "Best Directing (Comedy Picture)",
        "Best Directing (Dramatic Picture)", "Best Writing (Title Writing)",
        "Best Actress", "Best Actor in a Leading Role", "Best Actress in a Leading Role",
        "Best Actor in a Supporting Role", "Best Actress in a Supporting Role"
    }

    music_song_categories = {
        "Best Music (Song)", "Best Music (Original Song)"
    }

    CHARACTER_NAMES = {"Tonya Harding", "J. Paul Getty", "Katharine Graham"}

    def is_old_format(soup):
        table = soup.find("table", class_="wikitable")
        if not table:
            return False
        text = table.get_text().lower()
        return "outstanding picture" in text or "unique and artistic picture" in text

    def extract_person_and_movies_from_li(li):
        for tag in li.find_all("sup"):
            tag.decompose()
        person_tag = None
        for a in li.find_all("a", href=True):
            if not a.find_parent("i"):
                person_tag = a
                break
        if not person_tag:
            return []
        person_name = person_tag.get_text(strip=True)
        person_url = "https://en.wikipedia.org" + person_tag["href"]
        pairs = []
        for i_tag in li.find_all("i"):
            movie_link = i_tag.find("a", href=True)
            if movie_link:
                movie_name = movie_link.get_text(strip=True)
                movie_url = "https://en.wikipedia.org" + movie_link["href"]
                pairs.append((person_name, person_url, movie_name, movie_url))
        return pairs

    def parse_old_person_li(li, category_name, granted):
        nominations_local = []
        extracted = extract_person_and_movies_from_li(li)
        if extracted:
            person_name, person_url, movie_name, movie_url = extracted[0]
            nominations_local.append({
                "category": category_name,
                "movie_name": movie_name,
                "movie_url": movie_url,
                "is_winner": granted,
                "people_list": [(person_name, person_url)]
            })
        return nominations_local

    def get_relevant_li_tags(ul, category_name):
        li_tags = []
        for idx, top_li in enumerate(ul.find_all("li", recursive=False)):
            li_text = top_li.get_text(strip=True)
            is_bold = top_li.find("b") is not None
            has_star = '*' in li_text
            has_dagger = '‡' in li_text
            is_winner = is_bold and (has_star or has_dagger or (category_name in person_categories and idx == 0))
            top_li["data-winner"] = "1" if is_winner else "0"
            li_tags.append(top_li)
            nested_ul = top_li.find("ul", recursive=False)
            if nested_ul:
                for sub_li in nested_ul.find_all("li", recursive=False):
                    sub_li["data-winner"] = "0"
                    li_tags.append(sub_li)
        return [li for li in li_tags if li.get_text(strip=True)]

    def extract_category_names_from_row(tr):
        names = []
        for th in tr.find_all("th"):
            link = th.find("a")
            names.append(link.get_text(strip=True) if link else th.get_text(strip=True))
        return names

    old_format = is_old_format(soup)

    tables = soup.select("table.wikitable, table[class^='wikitable']")  
    for table in tables:
        rows = table.find_all("tr")
        if not rows:  
            for td in table.find_all("td", valign="top"):  
                category_div = td.find("div", style=lambda s: s and "#F9EFAA" in s)
                if not category_div:
                    continue
                category_name = category_div.get_text(strip=True)
                for li in td.find_all("li"):  
                    nomination = {
                        "category": category_name,
                        "movie_name": None,
                        "movie_url": None,
                        "is_winner": "‡" in li.get_text() or li.find("b") is not None,
                        "people_list": []
                    }
                    
                    if li.find("i"):
                        movie_link = li.find("i").find("a")
                        if movie_link:
                            nomination["movie_name"] = movie_link.get_text(strip=True)
                            nomination["movie_url"] = "https://en.wikipedia.org" + movie_link["href"]
                    person_link = li.find("a")
                    if person_link and not li.find("i"):
                        nomination["people_list"].append((
                            person_link.get_text(strip=True),
                            "https://en.wikipedia.org" + person_link["href"]
                        ))
                    nominations.append(nomination)
            continue  
        category_names = []

        for row in rows:
            ths = row.find_all("th")
            if ths:
                category_names = extract_category_names_from_row(row)
                continue

            tds = row.find_all("td", valign="top")
            tds = [td for td in row.find_all("td") if "vertical-align:top" in td.get("style", "")]
            if not tds:
                continue

            for col_idx, td in enumerate(tds):
                category_name = category_names[col_idx] if col_idx < len(category_names) else None

                if not category_name:
                    category_div = td.find("div", style=lambda val: val and "#F9EFAA" in val)
                    category_link = category_div.find("a") if category_div else None
                    category_name = category_link.get_text(strip=True) if category_link else (
                        category_div.get_text(strip=True) if category_div else None
                    )

                if not category_name:
                    continue

                ul_tag = td.find("ul")
                if not ul_tag:
                    continue

                li_tags = get_relevant_li_tags(ul_tag, category_name)
                for li in li_tags:
                    li_text = li.get_text(" ", strip=True)
                    if "@" in li_text:
                        continue
                    granted = 1 if li.get("data-winner") == "1" else 0

                    if category_name in music_song_categories:
                        i_tag = li.find("i")
                        if i_tag:
                            movie_link = i_tag.find("a", href=True)
                            if movie_link:
                                movie_name = movie_link.get_text(strip=True)
                                movie_url = "https://en.wikipedia.org" + movie_link["href"]
                                nominations.append({
                                    "category": category_name,
                                    "movie_name": movie_name,
                                    "movie_url": movie_url,
                                    "is_winner": granted,
                                    "people_list": []
                                })
                        continue

                    if old_format:
                        if category_name in person_categories:
                            nominations.extend(parse_old_person_li(li, category_name, granted))
                        else:
                            i_tag = li.find("i")
                            if i_tag:
                                movie_a = i_tag.find("a", href=True)
                                if movie_a:
                                    movie_name = movie_a.get_text(strip=True)
                                    if movie_name not in CHARACTER_NAMES:
                                        movie_url = "https://en.wikipedia.org" + movie_a["href"]
                                        nominations.append({
                                            "category": category_name,
                                            "movie_name": movie_name,
                                            "movie_url": movie_url,
                                            "is_winner": granted,
                                            "people_list": []
                                        })
                        continue

                    if category_name in person_categories:
                        pairs = extract_person_and_movies_from_li(li)
                        if pairs:
                            person_name, person_url, movie_name, movie_url = pairs[0]
                            nominations.append({
                                "category": category_name,
                                "movie_name": movie_name,
                                "movie_url": movie_url,
                                "is_winner": granted,
                                "people_list": [(person_name, person_url)]
                            })
                    else:
                        i_tag = li.find("i")
                        if i_tag:
                            movie_link = i_tag.find("a", href=True)
                            if movie_link:
                                movie_name = movie_link.get_text(strip=True)
                                if movie_name not in CHARACTER_NAMES:
                                    movie_url = "https://en.wikipedia.org" + movie_link["href"]
                                    nominations.append({
                                        "category": category_name,
                                        "movie_name": movie_name,
                                        "movie_url": movie_url,
                                        "is_winner": granted,
                                        "people_list": []
                                    })
                                continue  
                        elif li.find("i"):
                            movie_name = li.find("i").get_text(strip=True)
                            if movie_name not in CHARACTER_NAMES:
                                nominations.append({
                                    "category": category_name,
                                    "movie_name": movie_name,
                                    "movie_url": None,
                                    "is_winner": granted,
                                    "people_list": []
                                })
                        else:
                            movie_name = li_text.strip()
                            if movie_name not in CHARACTER_NAMES:
                                nominations.append({
                                    "category": category_name,
                                    "movie_name": movie_name,
                                    "movie_url": None,
                                    "is_winner": granted,
                                    "people_list": []
                                })


    return nominations

def get_award_year(soup):
    infobox = soup.find("table", class_="infobox vevent")
    if not infobox:
        return None
    rows = infobox.find_all("tr")
    for row in rows:
        header = row.find("th", class_="infobox-label")
        data = row.find("td", class_="infobox-data")
        if header and data:
            label_text = header.get_text(strip=True)
            if label_text == "Date":
                date_text = data.get_text(strip=True)
                match = re.search(r'(\d{4})', date_text)
                if match:
                    return int(match.group(1))
    return None


def get_hosts(soup):
    def parse_name_parts(name):
        parts = name.split()
        first = parts[0]
        last = parts[-1] if len(parts) > 1 else None
        middle = " ".join(parts[1:-1]) if len(parts) > 2 else None
        return first, middle, last

    infobox = soup.find("table", class_="infobox vevent")
    if not infobox:
        return []

    rows = infobox.find_all("tr")
    for row in rows:
        header = row.find("th", class_="infobox-label")
        data_cell = row.find("td", class_="infobox-data")
        if header and data_cell:
            label_text = header.get_text(strip=True)
            if "hosted by" in label_text.lower():
                for sup in data_cell.find_all("sup"):
                    sup.decompose()

                hosts = []
                a_tags = data_cell.find_all("a", href=True)

                for a in a_tags:
                    name = a.get_text(strip=True)

                    parent_text = a.parent.get_text(" ", strip=True).lower()

                    first, middle, last = parse_name_parts(name)
                    hosts.append((first, middle, last))

                if not a_tags and not hosts:
                    raw_text = data_cell.get_text(" ", strip=True)
                    parts = raw_text.split()
                    first = parts[0]
                    last = parts[-1] if len(parts) > 1 else None
                    middle = " ".join(parts[1:-1]) if len(parts) > 2 else None
                    hosts.append((first, middle, last))

                return hosts

    return []



def scrape_iteration_page(iteration_number):
    suffix = get_iteration(iteration_number)
    url = f"https://en.wikipedia.org/wiki/{suffix}_Academy_Awards"
    print(f"\n Scraping {url} ...")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    except Exception as e:
        print(f"Request failed: {e}")
        return None

    if response.status_code != 200:
        print(f"Failed to fetch page: Status code {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    with open(f"debug_{iteration_number}.html", "w", encoding="utf-8") as f:
        f.write(response.text)

    year = get_award_year(soup)
    hosts = get_hosts(soup)
    nominations = get_nominations(soup)

    print(f"Year extracted: {year}")
    print(f"Hosts extracted: {hosts}")
    print(f"Nominations extracted: {len(nominations)}")

    for nom in nominations[:5]:
        print("→", nom)

    return {
        "iteration_number": iteration_number,
        "year": year,
        "hosts": hosts,
        "nominations": nominations
    }




def populate_user_tables():
    print("Populating User Tables")

    db = get_db_connection()
    cursor = db.cursor()

    users = [
        ("user1@gmail.com", "Filmlover123", "M", date(2006, 3, 15), "USA"),
        ("user2@gmail.com", "Moviegoer2005", "F", date(1991, 7, 28), "UK"),
        ("user3@gmail.com", "AreegElkholy", "F", date(2011, 12, 1), "Canada")
    ]
    cursor.executemany("""
        INSERT IGNORE INTO User (Email, UserName, Gender, BirthDate, Country)
        VALUES (%s, %s, %s, %s, %s)
    """, users)
    print("Inserted users")

    people = [
        ("Robert", "Downey", date(1965, 4, 4)),
        ("Emma", "Stone", date(1988, 11, 6)),
        ("Cillian", "Murphy", date(1976, 5, 25)),
    ]
    for first, last, birth in people:
        cursor.execute("""
            INSERT IGNORE INTO People (FirstName, MiddleName, LastName, BirthDate, Country, DeathDate)
            VALUES (%s, NULL, %s, %s, %s, NULL)
        """, (first, last, birth, "USA"))
    print("Inserted people")

    nominations = [
        ("user1@gmail.com", 95, "Oppenheimer", date(2023, 7, 11), "Best Picture", None, None, None),

        ("user2@gmail.com", 96, "Guardians of the Galaxy Vol. 3", date(2023, 4, 22),
         "Best Actor in a Supporting Role", "Robert", "Downey", date(1965, 4, 4)),

        ("user3@gmail.com", 94, "Io capitano", date(2023, 9, 6),
         "Best Actor", "Cillian", "Murphy", date(1976, 5, 25))
    ]
    cursor.executemany("""
        INSERT IGNORE INTO UserNomination (
            UserEmail, IterationNumber, MovieName, MovieReleaseDate,
            CategoryName, PersonFirstName, PersonLastName, PersonBirthDate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, nominations)
    print("Inserted user nominations")

    db.commit()
    cursor.close()
    db.close()
    print("Done")



def main():
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    cursor.execute(
    "SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
)

    for iteration_number in range(1, 97): # LOOP THROUGH ALL 96 ITERATIONS CHANGE TO TEST YOUR CODE FIST!!!
        time.sleep(1)
        iteration_data = scrape_iteration_page(iteration_number)
        i_num = iteration_data["iteration_number"]
        i_year = iteration_data["year"]
        i_hosts = iteration_data["hosts"]
        nominations = iteration_data["nominations"]

        print(f"Iteration {i_num} - Year: {i_year}, Hosts: {i_hosts}")
        insert_iteration(cursor, i_num, i_year)

        for first_name, middle_name, last_name in i_hosts:
            insert_iteration_host(cursor, i_num, first_name, middle_name, last_name)


        for nom in nominations:
            cat_name = nom["category"]
            movie_name = nom["movie_name"]
            movie_url = nom["movie_url"]
            is_winner = nom["is_winner"]
            people_list = nom["people_list"]

            insert_nomination_category(cursor, cat_name)

            if movie_name:
                if movie_url:
                    movie_details = get_movie_details(movie_url)
                    if movie_details["name"]:
                        movie_name = movie_details["name"]
                    insert_movie(cursor, movie_name,
                               movie_details["release_date"],
                               movie_details["budget"],
                               movie_details["box_office"],
                               movie_details["runtime"],
                               movie_details["language"])

                    pc_list = get_production_companies(movie_details["soup"])
                    for pc_name in pc_list:
                        insert_production_company(cursor, pc_name)
                        insert_movie_production_company(cursor, movie_name,
                                                      movie_details["release_date"], pc_name)

                    roles_dict = get_movie_roles(movie_details["soup"])
                    for role, name_link_pairs in roles_dict.items():
                        for raw_person_name, person_url in name_link_pairs:
                            f_name, m_name, l_name = parse_name(raw_person_name)
                            insert_or_update_person_details(cursor, f_name, m_name, l_name, person_url)
                            
                            cursor.execute("""
                                SELECT BirthDate 
                                FROM People 
                                WHERE FirstName = %s AND LastName = %s 
                                ORDER BY 
                                    CASE 
                                        WHEN BirthDate IS NULL THEN 2
                                        ELSE 0 
                                    END
                                LIMIT 1
                            """, (f_name, l_name))


                            result = cursor.fetchone()
                            birth_date = result[0] if result and result[0] and result[0] != '0000-00-00' else None
                            birth_date = birth_date

                            insert_role_in_movie(cursor, role, movie_name,
                                               movie_details["release_date"],
                                               f_name, l_name, birth_date)
                            print(f"Inserted role: {role} for {f_name} {l_name} (Birth: {birth_date})")

                    movie_release_date = movie_details["release_date"]
                else:
                    print(f"Skipping movie without URL: {movie_name}")
                    movie_release_date = None

            else:
                movie_name = f"UnknownMovie_{cat_name}_{i_num}"
                insert_movie(cursor, movie_name, None, None, None, None, None)
                movie_release_date = None

            if people_list:
                for full_name, person_url in people_list:
                    f_name, m_name, l_name = parse_name(full_name)
                    insert_or_update_person_details(cursor, f_name, m_name, l_name, person_url)
                    
                    cursor.execute("""
                        SELECT BirthDate 
                        FROM People 
                        WHERE FirstName = %s AND LastName = %s 
                        ORDER BY 
                            CASE 
                                WHEN BirthDate IS NULL THEN 2
                                WHEN BirthDate = '0000-00-00' THEN 1
                                ELSE 0 
                            END
                        LIMIT 1
                    """, (f_name, l_name))

                    result = cursor.fetchone()
                    birth_date = result[0] if result and result[0] else '0000-00-00'


                    
                    insert_nomination(cursor, i_num, movie_name, movie_release_date,
                                    cat_name, f_name, l_name, birth_date, is_winner)

            else:
                insert_nomination(cursor, i_num, movie_name, movie_release_date,
                                cat_name, None, None, None, is_winner)

        db_conn.commit()
        print(f"Done with iteration {i_num}\n{'-'*50}")

    cursor.close()
    db_conn.close()
    print("Crawling and database population complete!")
    populate_user_tables()


main()