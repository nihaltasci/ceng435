import base64
import socket
import struct
import time
import os
import hashlib
import struct
import hashlib
from enum import Enum


#constants 
timeout = 0.5
server_address = ("172.17.0.2", 65432)
window_size = 25
MAX_RETRANSMISSIONS = 3
timer_start_time = None



# file types
class DataType(Enum):
    SMALL = 0
    LARGE = 1
    
    
# packet types
class PacketType(Enum):
    SEND_PACKET = 0
    RESEND_PACKET = 1
    ACK = 2
    FIN = 3
    FIN_ACK = 4
    

    
#packet structure, containing header information including checksum of data, type of packet, type of file, index of file from 0 to 9
#as well as the sequence number
class udp_packet(object):
    def __init__(self, data_type: DataType, packet_type: PacketType, sequence_num: int, checksum: str, file_index: int, data: bytes) -> None:
        self.data_type = data_type
        self.raw_data = data
        self.packet_type = packet_type
        self.sequence_num = sequence_num
        self.checksum = checksum
        self.file_index = file_index
    # serializes the header into encoded structure
    def serialize(self):
        # Use struct.pack to serialize the fields into a binary string
        return struct.pack('!BHH32sB', self.data_type.value, self.packet_type.value, self.sequence_num, self.checksum.encode('utf-8'), self.file_index) + self.raw_data
    # unpacks and decodes the serialized packet
    @classmethod
    def deserialize(cls, data):
        # Use struct.unpack to deserialize the binary string into fields
        expected_length = struct.calcsize('!BHH32sB')
        if len(data) < expected_length:
            raise ValueError(f"Insufficient data for deserialization. Expected {expected_length} bytes, got {len(data)} bytes.")

        unpacked_data = struct.unpack(f'!BHH32sB{len(data) - expected_length}s', data[:])
        data_type, packet_type, sequence_num, checksum_bytes, file_index, raw_data = unpacked_data

        checksum = checksum_bytes.decode('utf-8').rstrip('\x00')  # Remove null bytes if any
        return cls(DataType(data_type), PacketType(packet_type), sequence_num, checksum, file_index, raw_data)
    
def chunk_data(data, chunk_size = 1024):
    # divide data into chunks
    chunk_list = []
    for i in range(0, len(data), chunk_size):
        chunk_list.append(data[i:i + chunk_size])
    return chunk_list



def calculate_checksum(data):
    
    md5_hash = hashlib.md5()
    md5_hash.update(data)
    checksum = md5_hash.hexdigest()
    return checksum


def isCorrupted(data_packet):
    # takes the serialized packet as input and checks each member 
    # as well as the data for consistency
    deserialized_packet = udp_packet.deserialize(data_packet)
    data_type = deserialized_packet.data_type
    packet_type = deserialized_packet.packet_type
    sequence_num = deserialized_packet.sequence_num
    checksum = deserialized_packet.checksum
    raw_data = deserialized_packet.raw_data
    if not isinstance(data_type, (DataType.LARGE.__class__, DataType.SMALL.__class__)):
    # data type check
        return True
    if not isinstance(packet_type, (PacketType.ACK.__class__, PacketType.SEND_PACKET.__class__, PacketType.RESEND_PACKET.__class__, PacketType.FIN.__class__, PacketType.FIN_ACK.__class__)):
    # packet type check
        return True
    if not isinstance(sequence_num, (int)):
        #sequence number check
        return True
    # check wether the received checksum and the calculated checksum over raw_data are equal
    if checksum != calculate_checksum(raw_data):
        print(f"checksums are different for sequence number {sequence_num}")
        print(f"expected checksum: {checksum}")
        print(f"calculated checksum: {calculate_checksum(raw_data)}")
        return True
    return False








def start_timer():
    global timer_start_time
    timer_start_time = time.time()

def stop_timer():
    global timer_start_time
    if timer_start_time is not None:
        elapsed_time = time.time() - timer_start_time
        print(f"Timer stopped. Elapsed time: {elapsed_time} seconds")
        timer_start_time = None




objects_dir = "/root/objects"
# A function to read the files with their file index, the file index is attached to the packet as it is sent, 
#to store the files in consistent order
def read_files(objects_dir):
    files_large = []
    files_small = []

    for i, file_name in enumerate(os.listdir(objects_dir)):
        file_path = os.path.join(objects_dir, file_name)
        with open(file_path, "r") as file:
            file_content = file.read()
            if "large" in file_name and "md5" not in file_name:
                if "0" in file_name:
                    files_large.append((file_content, 0))
                elif "1" in file_name:
                    files_large.append((file_content, 1))
                elif "2" in file_name:
                    files_large.append((file_content, 2))
                elif "3" in file_name:
                    files_large.append((file_content, 3))
                elif "4" in file_name:
                    files_large.append((file_content, 4))
                elif "5" in file_name:
                    files_large.append((file_content, 5))
                elif "6" in file_name:
                    files_large.append((file_content, 6))
                elif "7" in file_name:
                    files_large.append((file_content, 7))
                elif "8" in file_name:
                    files_large.append((file_content, 8))
                elif "9" in file_name:
                    files_large.append((file_content, 9))
            elif "small" in file_name and "md5" not in file_name:
                if "0" in file_name:
                    files_small.append((file_content, 0))
                elif "1" in file_name:
                    files_small.append((file_content, 1))
                elif "2" in file_name:
                    files_small.append((file_content, 2))
                elif "3" in file_name:
                    files_small.append((file_content, 3))
                elif "4" in file_name:
                    files_small.append((file_content, 4))
                elif "5" in file_name:
                    files_small.append((file_content, 5))
                elif "6" in file_name:
                    files_small.append((file_content, 6))
                elif "7" in file_name:
                    files_small.append((file_content, 7))
                elif "8" in file_name:
                    files_small.append((file_content, 8))
                elif "9" in file_name:
                    files_small.append((file_content, 9))
                #only appends the files not checksums
    return (files_small, files_large)


# A function to prepare all the packets with their headers
def prepare_packets(files_small, files_large):
    data_packets = []
    next_sequence_number = 0
    small_ind = 0
    large_ind = 0
    while small_ind < len(files_small) or large_ind < len(files_large):
        if large_ind < len(files_large):
        # shuffles through the small and large packets to send one large then one small object
            large_file_index = files_large[large_ind][1]
            # each packet data is 3072 byes excluding the header size 
            chunks = chunk_data(files_large[large_ind][0], chunk_size = 3072)
            large_ind += 1
            for chunk in chunks:
                data_packets.append(udp_packet(DataType.LARGE, PacketType.SEND_PACKET, next_sequence_number, calculate_checksum(chunk.encode('utf-8')),large_file_index, chunk.encode('utf-8')))
                next_sequence_number += 1
        if small_ind < len(files_small):
            small_file_index = files_small[small_ind][1]
            chunks = chunk_data(files_small[small_ind][0], chunk_size = 3072)
            small_ind += 1
            for chunk in chunks:
                data_packets.append(udp_packet(DataType.SMALL, PacketType.SEND_PACKET, next_sequence_number, calculate_checksum(chunk.encode('utf-8')), small_file_index,chunk.encode('utf-8')))
                next_sequence_number += 1
    return data_packets



def gbn_sender():
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    base = 0
    #reading the files
    files_small, files_large = read_files(objects_dir)
    #preparing the packets
    data_packets = prepare_packets(files_small, files_large)
    print(f"length of data packets {len(data_packets)}")
    next_seq_num = 0
    #iterates through the packets and sends them one by one
    while base < len(data_packets):
    #for sequence numbers within the current window size, send them over
    #window size is 25
        for i in range(base, min(base + window_size, len(data_packets))):
            udp_socket.sendto(data_packets[i].serialize(), server_address)
            test = udp_packet.deserialize(data_packets[i].serialize())
            print(f"Sent packet with seq_num {i}")
            if base == next_seq_num:
            # if starting from the window base, set a timer
                start_timer()
                print(f"timer started for {base}")
            next_seq_num += 1 

        #after the window is all sent, wait for the ack messages
        while True:
            try:
                # set a timeout for a socket to prevent blocking behavior
                udp_socket.settimeout(timeout)
                ack_data, addr = udp_socket.recvfrom(1024)
                # check for ack message corruption 
                if not isCorrupted(ack_data):
                    deserialized = udp_packet.deserialize(ack_data)
                    if (deserialized.sequence_num + 1 == next_seq_num):
                        # if the ack for the end of the window is received, stop the timer  
                        # and move to the next window
                        base = next_seq_num
                        stop_timer()
                        break

                    
            except socket.timeout:
                # Timeout occurred, retransmit the packets in the current window
                print("Timeout occurred. Retransmitting packets.")
                next_seq_num = base
                break
    # after sending all the packets, sends the FIN message
    connection_open = True
    retransmissions = 0
    while True:
        if(not connection_open):
            break
        data_bytes = b""
        file_index = 10
        fin_packet = udp_packet(DataType.SMALL, PacketType.FIN, sequence_num=next_seq_num, checksum=calculate_checksum(data_bytes),file_index=file_index, data=data_bytes)
        udp_socket.sendto(fin_packet.serialize(), server_address)
        start_time = time.time()
        # if FIN_ACK was not recieved, it sends the FIN message 3 more times and closes the connection
        while retransmissions < MAX_RETRANSMISSIONS:
            try:
                udp_socket.settimeout(timeout)
                ack_data, addr = udp_socket.recvfrom(1024)
                data = udp_packet.deserialize(ack_data)
                if not isCorrupted(ack_data):
                    # stop_timer
                    print(f"fin ack received {data.packet_type}")
                    udp_socket.close()
                    print("Connection closed: client")
                    connection_open = False
                    break
            except socket.timeout:
                retransmissions += 1
                print("Finish message not acknowledged, resending...")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break

        if retransmissions == MAX_RETRANSMISSIONS:
            print(f"Max retransmission attempts reached. Unable to close the connection.")
            udp_socket.close()
            print("Connection closed: client")
            connection_open = False
            break
        
        


        






if __name__ == "__main__":
    gbn_sender()



