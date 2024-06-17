import socket
import struct
import time
import os
import hashlib
import matplotlib.pyplot as plt
import numpy as np
from enum import Enum


server_address = ("0.0.0.0", 65432)
# list to store the received packets
out_of_order_packs = []


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

def gbn_receiver():
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(server_address)

    expected_seq_num = 0
    print("server listening")


    while True:
        start_time = time.time()
        # the data size is 3096 bytes and approximately one byte for the header. The receiver has a buffer of 4096
        data_packet, addr = udp_socket.recvfrom(4096)
        end_time = time.time()
        #check for packet corruption
        if not isCorrupted(data_packet):
            packet = udp_packet.deserialize(data_packet)

            # Check for packet duplication
            if packet.sequence_num < expected_seq_num:
                # duplicate packet is received, send an ack message and discard it. 
                data = b""
                ack_packet = udp_packet(data_type=DataType.SMALL, packet_type=PacketType.ACK, sequence_num=packet.sequence_num, checksum=calculate_checksum(data), file_index=10, data=data)
                udp_socket.sendto(ack_packet.serialize(), addr)
                continue

            # Check if the received packet has the expected sequence number
            # if the packet is data-containing packet
            if packet.packet_type == PacketType.SEND_PACKET:
                if packet.sequence_num == expected_seq_num:
                    #store the packet for further processing
                    out_of_order_packs.append(packet)
                    data = b""
                    #send a packet with ACK message
                    ack_packet = udp_packet(data_type=DataType.SMALL, packet_type=PacketType.ACK, sequence_num=expected_seq_num, checksum=calculate_checksum(data), file_index=10, data=data)
                    udp_socket.sendto(ack_packet.serialize(), addr)
                    expected_seq_num += 1  # move the sequence number window forward
                else:
                # packets larger than expected sequence number recieved, discard, and wait for client timeout event
                    continue
            # if it is of FIN type
            else:
                print(f"received packet with type :{packet.packet_type}")
                data = b""
                # prepare a FIN packet, send it to client, and close the socket
                finAck_packet = udp_packet(data_type=DataType.SMALL, packet_type=PacketType.FIN_ACK, sequence_num=expected_seq_num, checksum=calculate_checksum(data),file_index=10, data=data)
                udp_socket.sendto(finAck_packet.serialize(), addr)
                udp_socket.close()
                print("connection closed: server")
                break

            

        else:
            # packet is corrupted
            print(f"Corrupted packet received. Discarding.")
    return 






def reconstruct_original_files(data_chunks):
    # sort data chunks based on their sequence numbers
    sorted_chunks = sorted(data_chunks, key=lambda chunk: chunk.sequence_num)

    # concatenate data chunks to reconstruct the original data
    reconstructed_data = "".join(chunk.raw_data.decode('utf-8') for chunk in sorted_chunks)

    return reconstructed_data

def save_to_files(reconstructed_data, j, data_type):
    # save the reconstructed data to the original files
    if data_type == DataType.LARGE:
        with open(f"large-{j}.obj", "w", encoding="utf-8") as file:
            file.write(reconstructed_data)
    else:
        with open(f"small-{j}.obj", "w", encoding="utf-8") as file:
            file.write(reconstructed_data)




if __name__ == "__main__":
    start = time.time()
    # receive the packets and store it in a list
    gbn_receiver()
    #post-processing of received packets
    j = 0
    turn = 0
    large_num = 0
    small_num = 0
    while j < len(out_of_order_packs):
        if out_of_order_packs[j].data_type == DataType.LARGE:
            # the received packet is large
            large_num = out_of_order_packs[j].file_index # get the file number of the data
            large_chunks = []
            while j < len(out_of_order_packs) and out_of_order_packs[j].data_type == DataType.LARGE:
            # add the data till a small packet is encountered
                large_chunks.append(out_of_order_packs[j])
                j += 1
            # reconstruct the file object from the recieved chunks and save it
            reconstructed_data = reconstruct_original_files(large_chunks)
            save_to_files(reconstructed_data, large_num , DataType.LARGE)
        else:
        # if the received packet is small, store it till a large is encountered
            small_num = out_of_order_packs[j].file_index
            small_chunks = []
            while j < len(out_of_order_packs) and out_of_order_packs[j].data_type == DataType.SMALL:
                small_chunks.append(out_of_order_packs[j])
                j += 1
            reconstructed_data = reconstruct_original_files(small_chunks)
            save_to_files(reconstructed_data, small_num , DataType.SMALL)
    print("end of saving files")
    end = time.time()
    print(f"total download time udp : {end - start}")
    

    





