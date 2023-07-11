import sqlite3
from flask import Flask, jsonify, request
from flask_cors import CORS
import jsonschema
from jsonschema import validate

# Configurando conexión a la Base de Datos
DB = 'mascotas.db'

def get_db_connection():
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def create_table():
    # Crear tabla si no existe
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS pets (
                pet_id INTEGER NOT NULL PRIMARY KEY,
                pet_name TEXT(30) NOT NULL,
                pet_description varchar(300) NOT NULL,
                pet_type TEXT(30) NOT NULL,
                pet_breed TEXT(30) NOT NULL,
                pet_sex TEXT(10) NOT NULL,
                pet_picture BLOB )
            """
        )

        conn.commit()
        cursor.close()
    except sqlite3.Error as error:
        print('Error creating tables:', error)
    finally:
        conn.close()

def create_db():
    conn = sqlite3.connect(DB)
    conn.close()
    create_table()

create_db()

#---------------------------------------------------------------
# Clase Mascotas
#---------------------------------------------------------------
class Pet:
    def __init__(self, pet_id, pet_name, pet_description, pet_type, pet_breed, pet_sex, pet_picture):
        self.pet_id = pet_id
        self.pet_name = pet_name.lower()
        self.pet_description = pet_description.lower()
        self.pet_type = pet_type.lower()
        self.pet_breed = pet_breed.lower()
        self.pet_sex = pet_sex.lower()
        self.pet_picture = pet_picture
        #Quitar None del atributo picture al final de las pruebas
    
    def modify_pet(self, new_name, new_description, new_type, new_breed, new_sex, new_picture):
        self.pet_name = new_name.lower()
        self.pet_description = new_description.lower()
        self.pet_type = new_type.lower()
        self.pet_breed = new_breed.lower()
        self.pet_sex = new_sex.lower()
        self.pet_picture = new_picture
    
    def to_dict(self):
        return {
            'id': self.pet_id,
            'name': self.pet_name,
            'description': self.pet_description,
            'type': self.pet_type,
            'breed': self.pet_breed,
            'sex': self.pet_sex,
            'image': self.pet_picture
        }

#---------------------------------------------------------------
# Clase Catalogo de Mascotas
#---------------------------------------------------------------
class Pet_Catalog:
    def __init__(self):
        self.connection = get_db_connection()
        self.cursor = self.connection.cursor()

    # Metodo para insertar mascota en la BD:
    def add_pet(self, pet_id, pet_name, pet_description, pet_type, pet_breed, pet_sex, pet_picture):
        try:
            pet_exist = self.find_pet_by_id(pet_id)
            if pet_exist:
                raise Exception('There is already a pet with that ID.')
            else:
                new_pet = Pet(pet_id, pet_name, pet_description, pet_type, pet_breed, pet_sex, pet_picture)
                self.cursor.execute("INSERT INTO pets VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (pet_id, pet_name, pet_description, pet_type, pet_breed, pet_sex, pet_picture))
                self.connection.commit()
                return jsonify({'message': 'The pet was created successfully.'}), 200
        except Exception as e:
            return jsonify({'message': str(e)}), 400

    # Metodo para buscar mascota por ID:
    def find_pet_by_id(self, pet_id):
        try:
            self.cursor.execute("SELECT * FROM pets WHERE pet_id = ?", (pet_id,))
            find = self.cursor.fetchone()
            if find:
                fetch_id, fetch_name, fetch_description, fetch_type, fetch_breed, fetch_sex, fetch_picture = find
                return Pet(fetch_id, fetch_name, fetch_description, fetch_type, fetch_breed, fetch_sex, fetch_picture)
            else:
                raise Exception('Pet not found.')
        except Exception as e:
            return None
    
    # Metodo para buscar mascota por nombre:
    def find_pet_by_name(self, pet_name):
        try:
            self.cursor.execute("SELECT * FROM pets WHERE pet_name = ?", (pet_name,))
            find = self.cursor.fetchall()
            pet_list = []
            if find:
                for row in find:
                    fetch_id, fetch_name, fetch_description, fetch_type, fetch_breed, fetch_sex, fetch_picture = row
                    pet = {
                        'id': fetch_id,
                        'name': fetch_name,
                        'description': fetch_description,
                        'type': fetch_type,
                        'breed': fetch_breed,
                        'sex': fetch_sex,
                        'image': fetch_picture
                    }
                    pet_list.append(pet)
                return jsonify(pet_list), 200
            else:
                raise Exception('Mascotas no encontradas.')
        except Exception as e:
            return jsonify({'message': str(e)}), 404

    # Metodo para buscar mascota por tipo:
    def find_pet_by_type(self, p_type):
        self.cursor.execute("SELECT * FROM pets WHERE pet_type = ?", (p_type,))
        find = self.cursor.fetchall()
        pet_list = []
        if find:
            for row in find:
                fetch_id, fetch_name, fetch_description, fetch_type, fetch_breed, fetch_sex, fetch_picture = row
                pet = {
                    'id': fetch_id,
                    'name': fetch_name,
                    'description': fetch_description,
                    'type': fetch_type,
                    'breed': fetch_breed,
                    'sex': fetch_sex,
                    'image': fetch_picture
                }
                pet_list.append(pet)
            return jsonify(pet_list), 200
        return jsonify({'message': 'Pets not found.'}), 404

    # Metodo para buscar mascota por raza:
    def find_pet_by_breed(self, pet_breed):
        self.cursor.execute("SELECT * FROM pets WHERE pet_breed = ?", (pet_breed,))
        find = self.cursor.fetchall()
        pet_list = []
        if find:
            for row in find:
                fetch_id, fetch_name, fetch_description, fetch_type, fetch_breed, fetch_sex, fetch_picture = row
                pet = {
                    'id': fetch_id,
                    'name': fetch_name,
                    'description': fetch_description,
                    'type': fetch_type,
                    'breed': fetch_breed,
                    'sex': fetch_sex,
                    'image': fetch_picture
                }
                pet_list.append(pet)
            return jsonify(pet_list), 200
        return jsonify({'message': 'Pets not found.'}), 404
    
    # Metodo para buscar mascota por sexo:
    def find_pet_by_sex(self, pet_sex):
        self.cursor.execute("SELECT * FROM pets WHERE pet_sex = ?", (pet_sex,))
        find = self.cursor.fetchall()
        pet_list = []
        if find:
            for row in find:
                fetch_id, fetch_name, fetch_description, fetch_type, fetch_breed, fetch_sex, fetch_picture = row
                pet = {
                    'id': fetch_id,
                    'name': fetch_name,
                    'description': fetch_description,
                    'type': fetch_type,
                    'breed': fetch_breed,
                    'sex': fetch_sex,
                    'image': fetch_picture
                }
                pet_list.append(pet)
            return jsonify(pet_list), 200
        return jsonify({'message': 'Pets not found.'}), 404

    # Metodo para listar todas las mascota de la BD:
    def select_all_pets(self):
        self.cursor.execute("SELECT * FROM pets")
        find = self.cursor.fetchall()
        pet_list = []
        if find:
            for row in find:
                pet_id, pet_name, pet_description, pet_type, pet_breed, pet_sex, pet_picture = row
                pet = {
                    'id': pet_id,
                    'name': pet_name,
                    'description': pet_description,
                    'type': pet_type,
                    'breed': pet_breed,
                    'sex': pet_sex,
                    'image': pet_picture
                }
                pet_list.append(pet)
            return jsonify(pet_list), 200
        return jsonify({'message': 'Pets not found.'}), 404
    
    # Buscar y listar todos los tipos de mascotas
    def get_pet_types(self):
        try:
            self.cursor.execute("SELECT DISTINCT pet_type FROM pets")
            types = self.cursor.fetchall()
            pet_types = [row[0] for row in types]
            return jsonify(pet_types), 200
        except Exception as e:
            return jsonify({'message': str(e)}), 500
    
    # Buscar y listar todos las razas de las mascotas
    def get_pet_breeds(self):
        try:
            self.cursor.execute("SELECT DISTINCT pet_breed FROM pets")
            breeds = self.cursor.fetchall()
            pet_breeds = [row[0] for row in breeds]
            return jsonify(pet_breeds), 200
        except Exception as e:
            return jsonify({'message': str(e)}), 500
        
    # Buscar y listar el sexo
    def get_pet_sex(self):
        try:
            self.cursor.execute("SELECT DISTINCT pet_sex FROM pets")
            sex = self.cursor.fetchall()
            pet_sex = [row[0] for row in sex]
            return jsonify(pet_sex), 200
        except Exception as e:
            return jsonify({'message': str(e)}), 500
    
    # Metodo para modificar una mascota en la BD:
    def edit_pet(self, pet_id, new_name, new_description, new_type, new_breed, new_sex, new_picture):
        try:
            pet = self.find_pet_by_id(pet_id)
            if pet:
                pet.modify_pet(new_name, new_description, new_type, new_breed, new_sex, new_picture)
                self.cursor.execute("UPDATE pets SET pet_name = ?, pet_description = ?, pet_type = ?, pet_breed = ?, pet_sex = ?, pet_picture = ? WHERE pet_id = ?",
                                    (new_name, new_description, new_type, new_breed, new_sex, new_picture, pet_id))
                self.connection.commit()
                return jsonify({'message': 'Pet modified successfully.'}), 200
            else:
                raise Exception('Pet not found.')
        except Exception as e:
            return jsonify({'message': str(e)}), 404

    # Metodo para eliminar una mascota de la BD:
    def delete_pet(self, pet_id):
        try:
            pet = self.find_pet_by_id(pet_id)
            if pet:
                self.cursor.execute("DELETE FROM pets WHERE pet_id = ?", (pet_id,))
                if self.cursor.rowcount > 0:
                    self.connection.commit()
                    return jsonify({'message': 'Pet successfully removed.'}), 200
                else:
                    raise Exception('An error occurred while deleting the pet.')
            else:
                raise Exception('Pet not found.')
        except Exception as e:
            return jsonify({'message': str(e)}), 500


# -------------------------------------------------------------------
# Creación de la API-Rest Flask
# -------------------------------------------------------------------

app = Flask(__name__)
CORS(app)

catalog = Pet_Catalog()

json_add_schema = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "type": {"type": "string"},
        "breed": {"type": "string"},
        "sex": {"type": "string"},
        "image": {"type": "string", "format": "binary"}
    },
    "required": ["id", "name", "description", "type", "breed", "sex", "image"]
}

json_upd_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "description": {"type": "string"},
        "type": {"type": "string"},
        "breed": {"type": "string"},
        "sex": {"type": "string"},
        "image": {"type": "string", "format": "binary"}
    },
    "required": ["name", "description", "type", "breed", "sex", "image"]
}

@app.route('/')
def index():
    return 'Catalogo de Mascotas'

@app.route('/pets', methods=['GET'])
def get_all_pets():
    return catalog.select_all_pets()

@app.route('/pets/<int:id>', methods=['GET'])
def get_pet_by_id(id):
    try:
        pet = catalog.find_pet_by_id(id)
        if pet:
            return jsonify({
                'id': pet.pet_id,
                'name': pet.pet_name,
                'description': pet.pet_description,
                'type': pet.pet_type,
                'breed': pet.pet_breed,
                'sex': pet.pet_sex,
                'image': pet.pet_picture
            }), 200
        else:
            raise Exception('Pet not found.')
    except Exception as e:
        return jsonify({'message': 'Pet not found.'}), 404

@app.route('/pets/find_name/<string:name>', methods=['GET'])
def get_pet_by_name(name):
    return catalog.find_pet_by_name(name)

@app.route('/pets/find_type/<string:pet_type>', methods=['GET'])
def get_pet_by_type(pet_type):
    return catalog.find_pet_by_type(pet_type)

@app.route('/pets/find_breed/<string:breed>', methods=['GET'])
def get_pet_by_breed(breed):
    return catalog.find_pet_by_breed(breed)

@app.route('/pets/find_sex/<string:sex>', methods=['GET'])
def get_pet_by_sex(sex):
    return catalog.find_pet_by_sex(sex)

@app.route('/pets', methods=['POST'])
def add_pet():
    try:
        data = request.get_json()
        validate(instance=data, schema=json_add_schema)
        
        pet_id = data['id']
        pet_name = data['name']
        pet_description = data['description']
        pet_type = data['type']
        pet_breed = data['breed']
        pet_sex = data['sex']
        pet_picture = data['image']
        return catalog.add_pet(pet_id, pet_name, pet_description, pet_type, pet_breed, pet_sex, pet_picture)
    
    except jsonschema.ValidationError as e:
        return jsonify({'message': 'Missing required fields in the JSON.'}), 400
    except Exception as e:
        return jsonify({'message': str(e)}), 400

@app.route('/pets/<int:pet_id>', methods=['PUT'])
def update_pet(pet_id):
    try:
        data = request.get_json()
        validate(instance=data, schema=json_upd_schema)
        
        new_name = data['name']
        new_description = data['description']
        new_type = data['type']
        new_breed = data['breed']
        new_sex = data['sex']
        new_picture = data['image']
        return catalog.edit_pet(pet_id, new_name, new_description, new_type, new_breed, new_sex, new_picture)
        
    except jsonschema.ValidationError as e:
        return jsonify({'message': 'Missing required fields in the JSON.'}), 400
    except Exception as e:
        return jsonify({'message': str(e)}), 400

@app.route('/pets/<int:pet_id>', methods=['DELETE'])
def delete_pet(pet_id):
    return catalog.delete_pet(pet_id)

@app.route('/pet-types', methods=['GET'])
def get_pet_types():
    return catalog.get_pet_types()

@app.route('/pet-breeds', methods=['GET'])
def get_pet_breeds():
    return catalog.get_pet_breeds()

@app.route('/pet-sex', methods=['GET'])
def get_pet_sex():
    return catalog.get_pet_sex()

if __name__ == '__main__':
    app.run()