CREATE DATABASE BD_Project;
GO

USE BD_Project;
GO

CREATE TABLE Category (
    CT_ID INT IDENTITY(1,1) PRIMARY KEY,
    CT_Category VARCHAR(30) UNIQUE NOT NULL
);

CREATE TABLE Channel (
    C_ID VARCHAR(40) PRIMARY KEY,
    C_Name VARCHAR(100),
    C_URL VARCHAR(200),
    C_Uploader VARCHAR(100)
);

CREATE TABLE Playlist (
    P_ID VARCHAR(50) PRIMARY KEY,
    P_Title VARCHAR(200),
    P_URL VARCHAR(200),
    P_C_ID VARCHAR(40),
    CONSTRAINT FK_Playlist_Channel FOREIGN KEY (P_C_ID)
        REFERENCES Channel (C_ID)
);

CREATE TABLE Video (
    V_ID VARCHAR(50) PRIMARY KEY,
    V_Title VARCHAR(200) NOT NULL,
    V_URL VARCHAR(200) NOT NULL,
    V_Duration BIGINT,
    V_Views BIGINT,
    V_Likes BIGINT,
    V_UploadDate DATE,
    V_Description NVARCHAR(MAX),
    V_Embed BIT,
    V_P_ID VARCHAR(50),
    CONSTRAINT FK_Video_Playlist FOREIGN KEY (V_P_ID)
        REFERENCES Playlist (P_ID),
    V_C_ID VARCHAR(40)
    CONSTRAINT FK_Video_Channel FOREIGN KEY (V_C_ID)
        REFERENCES Channel (C_ID),
);

CREATE TABLE VideoCategoryJunc (
    VC_CT INT NOT NULL,
    VC_V VARCHAR(50) NOT NULL,
    CONSTRAINT PK_VideoCategoryJunc PRIMARY KEY (VC_CT, VC_V),
    CONSTRAINT FK_VCJunc_Category FOREIGN KEY (VC_CT)
        REFERENCES Category (CT_ID),
    CONSTRAINT FK_VCJunc_Video FOREIGN KEY (VC_V)
        REFERENCES Video (V_ID)
);

CREATE TABLE Tags (
    Tag VARCHAR(50) NOT NULL,
    VT_V VARCHAR(50) NOT NULL,
    CONSTRAINT PK_Tags PRIMARY KEY (VT_V, Tag),
    CONSTRAINT FK_Tags_Video FOREIGN KEY (VT_V)
        REFERENCES Video (V_ID)
);

-- If you need to restart
ALTER DATABASE [BD_Project] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE BD_Project;
