(function () {
    function currentNavKey(pathname) {
        if (pathname === '/charts' || pathname === '/' || pathname.startsWith('/movies/')) return 'home';
        if (pathname === '/ranking' || pathname.startsWith('/ranking/')) return 'ranking';
        if (pathname === '/feed' || pathname.startsWith('/posts/')) return 'feed';
        if (pathname === '/reviews') return 'reviews';
        if (pathname === '/people' || pathname.startsWith('/users/')) return 'people';
        if (pathname === '/mypage' || pathname === '/stored') return 'profile';
        return '';
    }

    function activateSidebar() {
        const key = currentNavKey(window.location.pathname);
        document.querySelectorAll('[data-app-nav]').forEach((item) => {
            item.classList.toggle('active', item.dataset.appNav === key);
        });
    }

    function timeAgo(value) {
        if (!value) return '';
        const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
        if (seconds < 60) return '방금 전';
        if (seconds < 3600) return Math.floor(seconds / 60) + '분 전';
        if (seconds < 86400) return Math.floor(seconds / 3600) + '시간 전';
        return Math.floor(seconds / 86400) + '일 전';
    }

    function updateBadge(badge, count) {
        if (!badge) return;
        badge.textContent = count > 0 ? (count > 99 ? '99+' : String(count)) : '';
    }

    function renderNotifications(list, items) {
        if (!list) return;
        if (!items || !items.length) {
            list.innerHTML = '<div class="notif-empty">새 알림이 없습니다</div>';
            return;
        }
        list.innerHTML = items.map((item) => {
            const avatar = item.actorProfileImageUrl
                ? `<img src="${item.actorProfileImageUrl}" alt="">`
                : (item.actorNickname || '?')[0];
            const thumbnail = item.moviePosterUrl
                ? `<img class="notif-movie-thumb" src="${item.moviePosterUrl}" alt="">`
                : '';
            const text = item.notifText
                || (item.type === 'FOLLOW'
                    ? `<strong>${item.actorNickname}</strong>님이 팔로우했습니다`
                    : `<strong>${item.actorNickname}</strong>님이 활동했습니다`);
            return `<a class="notif-item${item.isRead ? '' : ' unread'}" href="${item.linkUrl || '#'}">
                <div class="notif-avatar">${avatar}</div>
                <div class="notif-content">
                    <div class="notif-text">${text}</div>
                    <div class="notif-time">${timeAgo(item.createdAt)}</div>
                </div>
                ${thumbnail}
            </a>`;
        }).join('');
    }

    function initializeNotifications() {
        if (document.body.dataset.appShellNotif !== 'true') return;

        const button = document.getElementById('notifBtn');
        const panel = document.getElementById('notifPanel');
        const badge = document.getElementById('notifBadge');
        const list = document.getElementById('notifList');
        const readAll = document.getElementById('notifReadAll');
        if (!button || !panel || !badge || !list) return;

        fetch('/api/notifications')
            .then((response) => response.ok ? response.json() : null)
            .then((data) => { if (data) updateBadge(badge, data.unreadCount); })
            .catch(() => {});

        button.addEventListener('click', (event) => {
            event.stopPropagation();
            const opening = panel.hidden;
            panel.hidden = !opening;
            if (!opening) return;
            fetch('/api/notifications')
                .then((response) => response.ok ? response.json() : null)
                .then((data) => {
                    if (!data) return;
                    updateBadge(badge, data.unreadCount);
                    renderNotifications(list, data.items);
                })
                .catch(() => {});
        });

        readAll?.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            fetch('/api/notifications/read-all', { method: 'POST' })
                .then(() => {
                    updateBadge(badge, 0);
                    document.querySelectorAll('.notif-item.unread').forEach((item) => item.classList.remove('unread'));
                })
                .catch(() => {});
        });

        document.addEventListener('click', (event) => {
            if (!panel.hidden && !panel.contains(event.target) && event.target !== button) {
                panel.hidden = true;
            }
        });
    }

    function initSkeletons() {
        document.querySelectorAll('.poster-shell, .rank-poster-wrap, .hp-poster-wrap').forEach((wrap) => {
            const img = wrap.querySelector('img');
            if (!img) return;
            if (img.complete && img.naturalWidth > 0) return;
            wrap.classList.add('img-loading');
            const done = () => wrap.classList.remove('img-loading');
            img.addEventListener('load', done, { once: true });
            img.addEventListener('error', done, { once: true });
        });
    }

    function initScrollHeader() {
        const topbar = document.querySelector('.app-topbar');
        if (!topbar) return;
        topbar.style.transition = 'transform 0.3s ease';
        let lastY = 0;
        let ticking = false;
        window.addEventListener('scroll', () => {
            if (ticking) return;
            ticking = true;
            requestAnimationFrame(() => {
                const y = window.scrollY;
                topbar.style.transform = (y > lastY && y > 80) ? 'translateY(-100%)' : 'translateY(0)';
                lastY = y;
                ticking = false;
            });
        }, { passive: true });
    }

    activateSidebar();
    initializeNotifications();
    initSkeletons();
    initScrollHeader();
})();
