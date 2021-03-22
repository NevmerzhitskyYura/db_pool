import dbpool as db

with db.pool_manager().manager() as conn:
    cursor = conn.cursor()
    query = "select * from product"
    cursor.execute(query)
    users = cursor.fetchall()

    for user in users:
        print(user)
