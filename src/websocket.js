import { Client } from '@stomp/stompjs';
import SockJS from 'sockjs-client';

const socketUrl = 'http://localhost:8083/online-game';

const stompClient = new Client({
  webSocketFactory: () => new SockJS(socketUrl),
  reconnectDelay: 5000,
  debug: (str) => console.log(str),
});

export default stompClient;
