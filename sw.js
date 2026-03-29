const BUILD_HASH = 'sarim-v29.03.2026-c';
const CACHE_NAME = 'app-' + BUILD_HASH;

const ASSETS_TO_CACHE = [
  '/sarim/',
  '/sarim/index.html',
  '/sarim/app.css',
  '/sarim/constants.js',
  '/sarim/business.js',
  '/sarim/sync.js',
  '/sarim/utilities.js',
  '/sarim/factory.js',
  '/sarim/customers.js',
  '/sarim/rep-sales.js',
  '/sarim/admin-data.js',
  '/sarim/manifest.json',
  '/sarim/192.png',
  '/sarim/512.png',

  '/sarim/sql-wasm.js',
  '/sarim/sql-wasm.wasm',
  '/sarim/sql.js'
];

const CDN_ASSETS_TO_PRECACHE = [
  'https://www.gstatic.com/firebasejs/10.7.1/firebase-app-compat.js',
  'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore-compat.js',
  'https://www.gstatic.com/firebasejs/10.7.1/firebase-auth-compat.js',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://accounts.google.com/gsi/client',
];

const FIREBASE_CDN_ORIGINS = [
  'https://www.gstatic.com',
  'https://unpkg.com',
  'https://accounts.google.com',
];

const SQLJS_CDN_ORIGIN = 'https://cdnjs.cloudflare.com';

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(ASSETS_TO_CACHE))
      .then(() => {

        return caches.open(CACHE_NAME).then((cache) =>
          Promise.allSettled(
            CDN_ASSETS_TO_PRECACHE.map(url =>
              cache.add(new Request(url, { mode: 'cors', credentials: 'omit' }))
                   .catch(() => {  })
            )
          )
        );
      })
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((key) => {
        if (key !== CACHE_NAME) return caches.delete(key);
      }))
    ).then(() => clients.claim())
  );
});

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

self.addEventListener('sync', (event) => {
  if (event.tag === 'offline-queue-sync') {
    event.waitUntil(
      self.clients.matchAll({ includeUncontrolled: true, type: 'window' }).then((clientList) => {
        if (clientList.length > 0) {
          clientList.forEach((client) => client.postMessage({ type: 'PROCESS_QUEUE' }));
        }
      })
    );
  }
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then((res) => { caches.open(CACHE_NAME).then((c) => c.put(event.request, res.clone())); return res; })
        .catch(() => caches.match('/sarim/index.html'))
    );
    return;
  }

  if (url.origin === SQLJS_CDN_ORIGIN && url.pathname.toLowerCase().includes('sql.js')) {
    return;
  }

  const isCdnAsset = CDN_ASSETS_TO_PRECACHE.some(u => event.request.url === u) ||
    FIREBASE_CDN_ORIGINS.some(origin => url.origin === origin);

  if (isCdnAsset) {
    event.respondWith(
      caches.open(CACHE_NAME).then((cache) =>
        cache.match(event.request).then((cached) => {
          if (cached) return cached;
          return fetch(event.request, { mode: 'cors', credentials: 'omit' })
            .then((res) => {
              if (res.ok) cache.put(event.request, res.clone());
              return res;
            })
            .catch(() => cached || new Response('', { status: 503 }));
        })
      )
    );
    return;
  }

  const isLocal = url.pathname.endsWith('.js') || url.pathname.endsWith('.css') ||
    url.pathname.endsWith('.json') || url.pathname.endsWith('.png') ||
    url.pathname.endsWith('.webp') || url.pathname.endsWith('.wasm');

  if (isLocal) {
    event.respondWith(
      caches.open(CACHE_NAME).then((cache) =>
        fetch(event.request).then((res) => {
          if (res.ok) cache.put(event.request, res.clone());
          return res;
        }).catch(() => cache.match(event.request, { ignoreMethod: true }))
      )
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cached) => {
      const fresh = fetch(event.request).then((res) => {
        if (res.ok) caches.open(CACHE_NAME).then((c) => c.put(event.request, res.clone()));
        return res;
      }).catch(() => null);
      return cached || fresh;
    })
  );
});
