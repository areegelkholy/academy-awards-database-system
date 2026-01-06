import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="sql.freedb.tech",
        user="freedb_areegelkholy",
        password="tT4222Qw$MJd%R$",
        database="freedb_academyawardsDB"
    )
