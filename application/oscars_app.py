from flask import Flask, render_template, request, redirect, url_for
from flask import jsonify
from datetime import datetime
from db_config import get_connection
import os

app = Flask(
    __name__,
    template_folder="htmls",
    static_folder="stuff"
)


person_categories = {
    "Best Directing", "Best Actor", "Best Director", "Best Directing (Comedy Picture)",
    "Best Directing (Dramatic Picture)", "Best Actress", "Best Actor in a Leading Role",
    "Best Actress in a Leading Role", "Best Actor in a Supporting Role",
    "Best Actress in a Supporting Role"
}

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT UserName FROM user WHERE Email = %s", (email,))
        user = cursor.fetchone()
        conn.close()

        if user:
            return redirect(url_for('dashboard', email=email))
        else:
            return "❌ Email not found. <a href='/login'>Try again</a>"

    return render_template('login.html')


@app.route('/dashboard/<email>')
def dashboard(email):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT UserName FROM user WHERE Email = %s", (email,))
    user = cursor.fetchone()
    conn.close()

    if not user:
        return redirect(url_for('login'))

    return render_template('dashboard.html', username=user[0], email=email)


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        username = request.form['username']
        gender = request.form['gender']
        birthdate = request.form['birthdate']
        country = request.form['country']

        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM user WHERE Email = %s", (email,))
            existing = cursor.fetchone()
            if existing:
                conn.close()
                return "❌ This email is already registered. <a href='/register'>Try again</a>"

            cursor.execute("""
                INSERT INTO user (Email, UserName, Gender, BirthDate, Country)
                VALUES (%s, %s, %s, %s, %s)
            """, (email, username, gender, birthdate, country))

            conn.commit()
            conn.close()
            return redirect(url_for('dashboard', email=email))

        except Exception as e:
            return f"❌ An error occurred: {e}"

    return render_template('register.html')


@app.route('/top-movies/<email>')
def top_movies(email):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.Name, m.ReleaseDate, COUNT(*) AS nomination_count
        FROM nomination n
        JOIN movie m ON n.MovieName = m.Name AND n.MovieReleaseDate = m.ReleaseDate
        GROUP BY m.Name, m.ReleaseDate
        ORDER BY nomination_count DESC
        LIMIT 10;
    """)
    top_movies = cursor.fetchall()
    conn.close()
    return render_template('top_movies.html', movies=top_movies, email=email)



@app.route('/nominate/<email>', methods=['GET', 'POST'])
def nominate(email):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM user WHERE Email = %s", (email,))
    if not cursor.fetchone():
        conn.close()
        return redirect(url_for('register'))

    if request.method == 'POST':
        iteration = int(request.form['iteration'])
        movie = request.form['movie'].split(" | ")
        category = request.form['category']

        movie_name = movie[0].strip()
        movie_date = movie[1].strip()

        if category in person_categories:
            person = request.form['person'].split(" | ")
            person_names = person[0].strip().split(" ", 1)
            firstname = person_names[0]
            lastname = person_names[1] if len(person_names) > 1 else ""
            birthdate = person[1].strip()


            cursor.execute("""
                INSERT INTO usernomination (UserEmail, IterationNumber, MovieName, MovieReleaseDate,
                    CategoryName, PersonFirstName, PersonLastName, PersonBirthDate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (email, iteration, movie_name, movie_date, category,
                  firstname, lastname, birthdate))
        else:
            cursor.execute("""
                INSERT INTO usernomination (UserEmail, IterationNumber, MovieName, MovieReleaseDate,
                    CategoryName, PersonFirstName, PersonLastName, PersonBirthDate)
                VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL)
            """, (email, iteration, movie_name, movie_date, category))

        conn.commit()
        conn.close()
        return render_template("success.html", email=email)


    cursor.execute("SELECT Number, Year FROM iteration ORDER BY Number")
    iterations = cursor.fetchall()
    selected_iteration = iterations[-1][0] if iterations else None

    cursor.execute("SELECT DISTINCT CategoryName FROM nominationcategory")
    categories = [row[0] for row in cursor.fetchall()]

    cursor.execute("""
        SELECT m.Name, m.ReleaseDate
        FROM movie m
        JOIN iteration i ON i.Number = %s
        WHERE YEAR(m.ReleaseDate) <= i.Year
        ORDER BY m.Name
    """, (selected_iteration,))
    movies = [f"{row[0]} | {row[1]}" for row in cursor.fetchall()]

    cursor.execute("""
        SELECT DISTINCT 
            CONCAT(p.FirstName, ' ', p.LastName, ' | ', p.BirthDate) AS Person,
            r.Role
        FROM people p
        JOIN roleinmovie r ON
            p.FirstName = r.PersonFirstName AND
            p.LastName = r.PersonLastName AND
            p.BirthDate = r.PersonBirthDate
        JOIN movie m ON m.Name = r.MovieName AND m.ReleaseDate = r.MovieReleaseDate
        JOIN iteration i ON i.Number = %s
        WHERE r.Role IN ('Actor', 'Director') AND YEAR(m.ReleaseDate) <= i.Year
    """, (selected_iteration,))
    people_raw = cursor.fetchall()
    people = {
        "Actor": [row[0] for row in people_raw if row[1] == "Actor"],
        "Director": [row[0] for row in people_raw if row[1] == "Director"]
    }

    conn.close()
    return render_template(
        'nominate.html',
        categories=categories,
        movies=movies,
        people=people,
        iterations=iterations,
        selected_iteration=selected_iteration,
        email=email,
        person_categories=list(person_categories)
    )



@app.route('/my-nominations/<email>')
def my_nominations(email):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT IterationNumber, MovieName, MovieReleaseDate, CategoryName,
               PersonFirstName, PersonLastName
        FROM usernomination
        WHERE UserEmail = %s
    """, (email,))
    
    nominations = cursor.fetchall()
    conn.close()

    return render_template('my_nominations.html', nominations=nominations, email=email)


@app.route('/person-awards/<email>', methods=['GET', 'POST'])
def person_awards(email):
    result = None
    people = []

    conn = get_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        role = request.form['role']
        name = request.form['name'].strip()

        if '|' in name:
            name = name.split('|')[0].strip()
        if ' ' in name:
            first_name, last_name = name.split(' ', 1)
        else:
            first_name = name
            last_name = ''

        cursor.execute("""
            SELECT BirthDate FROM people
            WHERE FirstName = %s AND LastName = %s
            LIMIT 1
        """, (first_name, last_name))
        birthdate_row = cursor.fetchone()

        if birthdate_row:
            birthdate = birthdate_row[0]

            cursor.execute("""
                SELECT 
                    COUNT(*) AS total_nominations,
                    SUM(CASE WHEN n.Granted = 1 THEN 1 ELSE 0 END) AS total_oscars
                FROM nomination n
                JOIN roleinmovie r ON
                    n.MovieName = r.MovieName AND
                    n.MovieReleaseDate = r.MovieReleaseDate AND
                    n.PersonFirstName = r.PersonFirstName AND
                    n.PersonLastName = r.PersonLastName AND
                    n.PersonBirthDate = r.PersonBirthDate
                WHERE 
                    r.Role = %s AND
                    r.PersonFirstName = %s AND
                    r.PersonLastName = %s AND
                    r.PersonBirthDate = %s
            """, (role, first_name, last_name, birthdate))

            data = cursor.fetchone()
            if data:
                nominations, oscars = data
                result = {
                    'name': name,
                    'role': role,
                    'nominations': nominations,
                    'oscars': oscars
                }

    cursor.execute("""          
        SELECT DISTINCT 
        CONCAT(FirstName, ' ', LastName) AS FullName
        FROM people
        ORDER BY FullName

    """)
    people = [row[0] for row in cursor.fetchall()]

    conn.close()

    return render_template("nominations_summary.html", result=result, people=people, email=email)




@app.route('/stats/actor-countries')
def best_actor_category():
    email = request.args.get('email')

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT p.Country, COUNT(*) AS wins
        FROM nomination n
        JOIN people p ON 
            n.PersonFirstName = p.FirstName AND
            n.PersonLastName = p.LastName AND
            n.PersonBirthDate = p.BirthDate
        WHERE n.CategoryName = 'Best Actor'
        AND n.Granted = 1
        GROUP BY p.Country
        ORDER BY wins DESC
        LIMIT 5;
    """)
    top_countries = cursor.fetchall()
    conn.close()

    return render_template('best_actor_category.html', top_countries=top_countries, email=email)


@app.route('/stats/staff-by-country')
def staff_by_country():
    country = request.args.get('country')
    email = request.args.get('email')
    results = []
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT Country FROM people WHERE Country IS NOT NULL AND Country != '' ORDER BY Country;")
    countries = [row[0] for row in cursor.fetchall()]

    if country:
        cursor.execute("""
                    SELECT 
            CONCAT(p.FirstName, ' ', p.LastName) AS FullName,
            n.CategoryName,
            COUNT(*) AS TotalNominations,
            SUM(CASE WHEN n.Granted = 1 THEN 1 ELSE 0 END) AS Wins
        FROM nomination n
        JOIN people p ON 
            n.PersonFirstName = p.FirstName AND 
            n.PersonLastName = p.LastName AND 
            n.PersonBirthDate = p.BirthDate
        WHERE p.Country = %s
        GROUP BY p.FirstName, p.LastName, p.BirthDate, n.CategoryName
        ORDER BY Wins DESC, TotalNominations DESC;
        """, (country,))
        results = cursor.fetchall()

    conn.close()

    return render_template(
        'staff_by_country.html',
        results=results,
        country=country,
        countries=countries,
        email=email
    )


@app.route('/dream-team')
def dream_team():
    email = request.args.get('email')  
    conn = get_connection()
    cursor = conn.cursor()

    roles = {
        "Director": ["Best Directing", "Best Director", "Best Directing (Comedy Picture)", "Best Directing (Dramatic Picture)"],
        "Leading Actor": ["Best Actor", "Best Actor in a Leading Role"],
        "Leading Actress": ["Best Actress", "Best Actress in a Leading Role"],
        "Supporting Actor": ["Best Actor in a Supporting Role"],
        "Supporting Actress": ["Best Actress in a Supporting Role"],
        "Producer": ["Best Picture"]
    }

    dream_team = []

    for title, categories in roles.items():
        placeholders = ', '.join(['%s'] * len(categories))

        query = f"""
            SELECT 
                p.FirstName, p.LastName, p.BirthDate, COUNT(*) AS Wins
            FROM nomination n
            JOIN people p ON 
                n.PersonFirstName = p.FirstName AND
                n.PersonLastName = p.LastName AND
                n.PersonBirthDate = p.BirthDate
            WHERE 
                n.CategoryName IN ({placeholders}) AND 
                n.Granted = 1 AND 
                p.DeathDate IS NULL
            GROUP BY p.FirstName, p.LastName, p.BirthDate
            ORDER BY Wins DESC
            LIMIT 1;
        """

        cursor.execute(query, categories)
        result = cursor.fetchone()

        if result:
            dream_team.append({
                "role": title,
                "name": f"{result[0]} {result[1]}",
                "birthdate": result[2],
                "wins": result[3]
            })

    conn.close()
    return render_template('dream_team.html', dream_team=dream_team, email=email)


@app.route('/stats/top-companies')
def top_companies():
    email = request.args.get('email')  
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            m.ProductionCompanyName,
            COUNT(*) AS WonOscars
        FROM nomination n
        JOIN movieproductioncompany m ON 
            n.MovieName = m.MovieName AND 
            n.MovieReleaseDate = m.MovieReleaseDate
        WHERE n.Granted = 1 AND m.ProductionCompanyName IS NOT NULL
        GROUP BY m.ProductionCompanyName
        ORDER BY WonOscars DESC
        LIMIT 5;
    """)

    top_companies = cursor.fetchall()
    conn.close()

    return render_template('top_companies.html', top_companies=top_companies, email=email)

@app.route('/stats/non-english')
def non_english_oscar_winners():
    email = request.args.get('email')

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT 
            m.Name, 
            m.ReleaseDate, 
            m.Language
        FROM nomination n
        JOIN movie m ON 
            n.MovieName = m.Name AND 
            n.MovieReleaseDate = m.ReleaseDate
        WHERE 
            LOWER(m.Language) NOT LIKE '%english%' AND 
            LOWER(m.Language) NOT LIKE '%american%' AND 
            n.Granted = 1
        ORDER BY m.ReleaseDate DESC;
    """)

    non_english_movies = cursor.fetchall()
    conn.close()

    return render_template('non_english_oscar_winners.html', non_english_movies=non_english_movies, email=email)

@app.route('/stats/top-user-nominated', methods=['GET'])
def top_user_nominated_movies():
    email = request.args.get('email')

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT  
            un.IterationNumber AS Iteration_Number,
            i.Year AS Year,
            un.CategoryName,
            un.MovieName,
            COUNT(*) AS TotalNominations
        FROM usernomination un 
        JOIN iteration i ON i.Number = un.IterationNumber
        GROUP BY un.IterationNumber, i.Year, un.CategoryName, un.MovieName
        ORDER BY un.IterationNumber DESC, un.CategoryName, TotalNominations DESC;
    """)

    nominations = cursor.fetchall()
    conn.close()

    return render_template(
        'top_user_nominated_movies.html', 
        nominations=nominations, 
        email=email
    )



if __name__ == '__main__':
    app.run(debug=False)
