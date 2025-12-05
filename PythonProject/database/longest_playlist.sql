SELECT P_Title, SUM(ISNULL(V_Duration,0)) AS total_duration
FROM Playlist
JOIN Video ON Playlist.P_ID = Video.V_P_ID
GROUP BY P_Title
ORDER BY total_duration DESC