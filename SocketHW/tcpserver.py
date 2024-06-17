import socket
import time

def receive_objects(client_socket, num_objects):
    received_objects = []
    for i in range(num_objects*4):
        print(f"Waiting for obj {i + 1}")
        object_data = b""
        # receive data in chuncks until a null byte is encountered
        while True:
            chunk = client_socket.recv(1024)
            if not chunk:
                break
            object_data += chunk
            if b"\x00" in chunk:
                break

        try:
                # decode the received object data
            if object_data.strip(b"\x00"):
                received_object = object_data.decode('utf-8').rstrip("\x00")
                print(f"Obj {i + 1} received")
                # append received object to the list
                received_objects.append(received_object)
                # send acknowledgment to the client
                acknowledgment = f"Acknowledgment for object {i + 1}".encode()
                client_socket.sendall(acknowledgment)
            else:
                print(f"Obj {i + 1} is empty or incomplete, skipping...")
        except UnicodeDecodeError as e:
            print(f"Error decoding obj {i + 1}: {e}")
            acknowledgment = f"Error decoding obj {i + 1}: {e}".encode()
            client_socket.sendall(acknowledgment)

    return received_objects

# function to save received objects to files
def save_to_file(received_objects):
        file_order = [
                "large-{}.obj",
                "small-{}.obj",
                "large-{}.obj.md5",
                "small-{}.obj.md5"
        ]

        ordered_files = []

        for i in range(10):
                for file_pattern in file_order:
                        file_name = file_pattern.format(i)
                        ordered_files.append(file_name)

        for i, obj in enumerate(received_objects):
                file_name = ordered_files[i]
                with open(file_name,"w") as output_file:
                        output_file.write(obj)

# main function to set up the server and handle connections
def main():
        # create a tcp socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # bind the socket to a specific address and port
    server_socket.bind(("0.0.0.0", 65432))
    # listen for incoming connections
    server_socket.listen(1)

    print("Server listening on port 65432")
        #accept connections and process data
    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")
        start = time.time()
        num_objects = 10
        # receive objects from the client
        received_objects = receive_objects(client_socket, num_objects)
                # save received objects to files
        save_to_file(received_objects)
                # close the client socket
        client_socket.close()
        print("Server OK")
        end = time.time()
        print(f"total download time tcp: {end - start}")
if __name__ == "__main__":
    main()
