<template>
  <div class="app">
    <h1>Search For Unlisted Videos</h1>

    <form class="search-bar" id="search-form" @submit.prevent="searchVideos()">
      <input
        id="query"
        type="text"
        placeholder="Search YouTube..."
        autocomplete="off"
        v-model="searchString"
      />
      <button type="submit">
        Search
      </button>
    </form>

    <div class="status">{{ status }}</div>

    <div class="results">
      <article
        v-for="video in searchResults"
        :key="video.id"
        class="video"
      >
        <a
          class="thumb-link"
          :href="getWatchUrl(video)"
          target="_blank"
          rel="noopener noreferrer"
        >
          <div
            class="thumb"
            :style="{ backgroundImage: 'url(' + getThumbUrl(video) + ')' }"
          ></div>
        </a>
        <div class="video-title">{{ getTitle(video) }}</div>
        <div class="video-meta">{{ getChannel(video) }}</div>
        <a
          class="video-link"
          :href="getWatchUrl(video)"
          target="_blank"
          rel="noopener noreferrer"
        >
          Open on YouTube
        </a>
      </article>
    </div>

    <div
      class="pagination"
      v-if="searchResults.length > 0 && totalPages > 1"
    >
      <button
        type="button"
        class="page-btn"
        :disabled="!hasPrev"
        @click="searchVideos(page - 1)"
      >
        Previous
      </button>
      <span class="page-info">
        Page {{ page }} of {{ totalPages }}
      </span>
      <button
        type="button"
        class="page-btn"
        :disabled="!hasNext"
        @click="searchVideos(page + 1)"
      >
        Next
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import axios from 'axios';

interface SqlMetadata {
  videoId: string;
  videoTitle: string;
  uploader: string;
  channel: string;
  availability?: string | null;
  playlistTitle?: string | null;
}

interface YtMetadata {
  title?: string | null;
  description?: string | null;
  tags?: string[] | null;
}

interface Video {
  id: string;
  videoId: string;
  videoUrl: string;
  sqlData?: SqlMetadata | null;
  ytMetadata?: YtMetadata | null;
}

interface SearchResponse {
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
  results: Video[];
}

const apiBaseURL: string = 'https://localhost:7052/';
const searchString = ref<string>('');
const searchResults = ref<Video[]>([]);
const status = ref<string>('Type anything and press Enter to see results.');
const page = ref<number>(1);
const pageSize = ref<number>(12);
const total = ref<number>(0);
const totalPages = ref<number>(0);

const hasPrev = computed<boolean>(() => page.value > 1);
const hasNext = computed<boolean>(() =>
  totalPages.value ? page.value < totalPages.value : false
);

function getTitle(video: Video): string {
  return video.sqlData?.videoTitle || video.ytMetadata?.title || 'Untitled';
}

function getChannel(video: Video): string {
  return video.sqlData?.channel || video.sqlData?.uploader || 'Unknown channel';
}

function getWatchUrl(video: Video): string {
  return video.videoUrl || `https://www.youtube.com/watch?v=${encodeURIComponent(video.videoId)}`;
}

function getThumbUrl(video: Video): string {
  return `https://img.youtube.com/vi/${encodeURIComponent(video.videoId)}/hqdefault.jpg`;
}

function searchVideos(targetPage?: number) {
  const q = searchString.value.trim();
  if (!q) {
    status.value = 'Please enter a search term.';
    searchResults.value = [];
    page.value = 1;
    total.value = 0;
    totalPages.value = 0;
    return;
  }

  if (targetPage && targetPage > 0) {
    page.value = targetPage;
  } else {
    page.value = 1;
  }

  status.value = `Searching for “${q}”...`;

  axios
    .get<SearchResponse>(`${apiBaseURL}api/search/videos`, {
      params: {
        q,
        page: page.value,
        pageSize: pageSize.value
      }
    })
    .then(response => {
      const data = response.data;
      searchResults.value = data.results || [];
      page.value = data.page;
      pageSize.value = data.pageSize;
      total.value = data.total;
      totalPages.value = data.totalPages;

      if (searchResults.value.length === 0) {
        status.value = `No results for “${q}”.`;
      } else {
        status.value = `Showing results for “${q}”.`;
      }
    })
    .catch(() => {
      status.value = 'Error loading results.';
      searchResults.value = [];
      total.value = 0;
      totalPages.value = 0;
    });
}
</script>

<style scoped lang="less">
.app {
  max-width: 960px;
  margin: 0 auto;
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #f7f7f7;
  color: #111;
  padding: 24px;

  h1 {
    font-size: 1.5rem;
    margin-bottom: 1rem;
    font-weight: 500;
  }

  .search-bar {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 0.75rem;

    input {
      flex: 1;
      padding: 0.55rem 0.9rem;
      border-radius: 999px;
      border: 1px solid #ccc;
      font-size: 0.95rem;
      outline: none;
      background: #fff;
    }

    input:focus {
      border-color: #000;
    }

    button {
      border: none;
      border-radius: 999px;
      padding: 0.55rem 1.1rem;
      font-size: 0.9rem;
      background: #111;
      color: #fff;
      cursor: pointer;
    }
  }

  .status {
    font-size: 0.8rem;
    color: #555;
    min-height: 1em;
    margin-bottom: 0.5rem;
  }

  .results {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 0.75rem;
  }

  .video {
    background: #fff;
    border-radius: 0.5rem;
    padding: 0.5rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }

  .thumb-link {
    display: block;
    text-decoration: none;
  }

  .thumb {
    position: relative;
    width: 100%;
    aspect-ratio: 16 / 9;
    border-radius: 0.35rem;
    overflow: hidden;
    background-size: cover;
    background-position: center;
    background-color: #000;
  }

  .thumb::after {
    content: "▶";
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 2.5rem;
    color: #fff;
    text-shadow: 0 2px 6px rgba(0,0,0,0.7);
  }

  .video-title {
    font-size: 0.85rem;
    line-height: 1.3;
  }

  .video-meta {
    font-size: 0.75rem;
    color: #777;
  }

  .video-link {
    font-size: 0.75rem;
    color: #0066cc;
    text-decoration: none;
  }

  .video-link:hover {
    text-decoration: underline;
  }

  .pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    margin-top: 0.75rem;
    font-size: 0.8rem;
  }

  .page-btn {
    border: none;
    border-radius: 999px;
    padding: 0.35rem 0.9rem;
    font-size: 0.8rem;
    background: #111;
    color: #fff;
    cursor: pointer;
  }

  .page-btn:disabled {
    opacity: 0.5;
    cursor: default;
  }

  .page-info {
    color: #555;
  }
}
</style>
