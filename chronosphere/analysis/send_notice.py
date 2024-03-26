import socket

def send_event_to_server(object):
    # Server address and port
    PORT = 12345  # Use the same port number as in your server-side script

    print(object.MONITOR_HOST)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((object.MONITOR_HOST, PORT))

    # Send the event data to the server
    event_data = 'play_sound'
    client_socket.send(event_data.encode())

    client_socket.close()