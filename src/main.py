from database import Database

def main():
    db = Database()
    print("mini_rdbms interactive shell")
    print("Type 'exit' to quit")

    while True:
        command = input("mini_rdbms> ").strip()

        if command.lower() == "exit":
            print("Goodbye!")
            break

        # VERY SIMPLE CREATE TABLE support
        if command.lower().startswith("create table"):
            try:
                # Example: CREATE TABLE todos (id, task)
                parts = command.replace("(", "").replace(")", "").split()
                table_name = parts[2]
                columns = parts[3:]

                result = db.create_table(table_name, columns)
                print(result)
            except Exception as e:
                print("Error:", e)
        else:
            print("Unsupported command (yet)")

if __name__ == "__main__":
    main()
