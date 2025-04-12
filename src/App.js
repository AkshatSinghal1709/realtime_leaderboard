import React, { useEffect, useState } from 'react';
import stompClient from './websocket';

function App() {
  const [leaderboard, setLeaderboard] = useState([]);

  useEffect(() => {
    stompClient.onConnect = () => {
      console.log('Connected to WebSocket');

      stompClient.subscribe('/live_updates/leaderboard', (message) => {
        const data = JSON.parse(message.body);
        console.log('Received leaderboard:', data);
        setLeaderboard(data.users); // assuming `users` is an array
      });
    };

    stompClient.activate();

    return () => {
      stompClient.deactivate();
    };
  }, []);

  return (
    <div>
      <h1>Live Leaderboard</h1>
      <ul>
        {leaderboard.map((user, index) => (
          <li key={index}>
            #{user.rank} - {user.nickname} ({user.score})
          </li>
        ))}
      </ul>
    </div>
  );
}

export default App;
