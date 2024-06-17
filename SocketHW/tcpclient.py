import socket
import time
import os

objects = []
objects_dir = "/root/objects" # directory containing objects which client send

# define the file order based on the specifie sequence
file_order = [
        "large-{}.obj",
        "small-{}.obj",
        "large-{}.obj.md5",
        "small-{}.obj.md5"
]

# exclude file:'generateobjects.sh'
excluded_files = {'generateobjects.sh'}

ordered_files = []


for i in range(10):
    for file_pattern in file_order:
        file_name = file_pattern.format(i)
        if file_name not in excluded_files:
            file_path = os.path.join(objects_dir,file_name)
            ordered_files.append(file_path)


for file_path in ordered_files:
    with open(file_path,"r") as file:
        file_content = file.read()
        objects.append(file_content)


print("Number of files to be sent: {}".format(len(objects)))

# function to send objects to the server
def send_objects(client_socket, objects):
    for i, obj in enumerate(objects):
        # encode file content ( and null byte)
        serialized_data = obj.encode('utf-8') + b"\x00"
        #send serialized data to the server
        time.sleep(1)
        client_socket.sendall(serialized_data)
        # wait for acknowledgment from the server
        acknowledgment = client_socket.recv(1024)
        print(f"Received acknowledgment for object {i + 1}: {acknowledgment.decode()}")

# main function to establish connection and send files
def main():
        # create a tcp socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # connect to the server
    client_socket.connect(("172.17.0.2", 65432))

    # send files to the server
    send_objects(client_socket, objects)
        # close the socket
    client_socket.close()
    print("Client OK")

if __name__ == "__main__":
    main()
