SELECT C_Name, COUNT(Video.V_ID) AS video_count
FROM Channel
JOIN Video ON Channel.C_ID = Video.V_C_ID
GROUP BY C_Name
ORDER BY video_count DESC