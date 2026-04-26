
async def extract_page_markdown() -> str:
	script = r"""
		({ sections }) => {
			const ATTRIBUTE_WHITELIST = new Set([
				'id', 'name', 'type', 'role', 'placeholder', 'value', 'title', 'alt',
				'aria-label', 'aria-expanded', 'aria-haspopup', 'href',
				'data-url', 'data-href', 'data-link', 'data-ep-wrapper-link'
			]);
		const FOOTER_ROLE_NAMES = new Set(['contentinfo']);
		const HEADER_ROLE_NAMES = new Set(['banner']);
		const enabledSections = new Set(sections);
		const selectorMap = {};

		function stableId(path) {
			let hash = 0;
			for (let index = 0; index < path.length; index += 1) {
				hash = ((hash << 5) - hash + path.charCodeAt(index)) | 0;
			}
			return Math.abs(hash % 9000) + 1000;
		}

		function cleanText(value) {
			return (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
		}

		function cleanBlockText(value) {
			return (value || '')
				.replace(/\u00a0/g, ' ')
				.replace(/[ \t\f\r]+/g, ' ')
				.split('\n')
				.map((line) => line.trim())
				.join('\n')
				.replace(/\n{3,}/g, '\n\n')
				.trim();
		}

		function escapeMarkdown(value) {
			return String(value || '').replace(/([\\`*_{}\[\]<>])/g, '\\$1');
		}

		function escapeMarkdownUrl(value) {
			return String(value || '').replace(/[()\s]/g, (char) => encodeURIComponent(char));
		}

		function toAbsoluteUrl(value) {
			const raw = cleanText(value || '');
			if (!raw) return '';
			try {
				return new URL(raw, window.location.href).href;
			} catch (_error) {
				return raw;
			}
		}

		function isVisible(el) {
			if (!(el instanceof Element)) return false;
			const tag = el.tagName.toLowerCase();
			if (['script', 'style', 'noscript', 'template', 'svg', 'canvas'].includes(tag)) return false;
			if (el.hasAttribute('hidden') || el.getAttribute('aria-hidden') === 'true') return false;

			const style = window.getComputedStyle(el);
			if (!style) return false;
			if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') === 0) {
				return false;
			}

			const rect = el.getBoundingClientRect();
			return (rect.width > 0 && rect.height > 0) || cleanText(el.textContent).length > 0;
		}

		function isFooterLike(el) {
			const tag = el.tagName.toLowerCase();
			const role = (el.getAttribute('role') || '').toLowerCase();
			const id = (el.getAttribute('id') || '').toLowerCase();
			const className = typeof el.className === 'string' ? el.className.toLowerCase() : '';
			const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
			return tag === 'footer'
				|| FOOTER_ROLE_NAMES.has(role)
				|| id.includes('footer')
				|| className.includes('footer')
				|| ariaLabel.includes('footer');
		}

		function isHeaderLike(el) {
			const tag = el.tagName.toLowerCase();
			const role = (el.getAttribute('role') || '').toLowerCase();
			const id = (el.getAttribute('id') || '').toLowerCase();
			const className = typeof el.className === 'string' ? el.className.toLowerCase() : '';
			const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
			return tag === 'header'
				|| HEADER_ROLE_NAMES.has(role)
				|| id.includes('header')
				|| className.includes('header')
				|| ariaLabel.includes('header');
		}

		function isNavLike(el) {
			const tag = el.tagName.toLowerCase();
			const role = (el.getAttribute('role') || '').toLowerCase();
			const id = (el.getAttribute('id') || '').toLowerCase();
			const className = typeof el.className === 'string' ? el.className.toLowerCase() : '';
			const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
			return tag === 'nav'
				|| role === 'navigation'
				|| id.includes('nav')
				|| className.includes('nav')
				|| ariaLabel.includes('navigation');
		}

		function getSectionType(el) {
			if (isHeaderLike(el)) return 'header';
			if (isFooterLike(el)) return 'footer';
			return 'body';
		}

		function isAllowedSection(el) {
			let current = el;
			while (current instanceof Element) {
				if (isNavLike(current)) return false;
				const sectionType = getSectionType(current);
				if (sectionType !== 'body') return enabledSections.has(sectionType);
				current = current.parentElement;
			}
			return enabledSections.has('body');
		}

		function buildDomPath(el) {
			const segments = [];
			let current = el;
			while (current && current instanceof Element) {
				const tag = current.tagName.toLowerCase();
				const parent = current.parentElement;
				if (!parent) {
					segments.push(`${tag}[0]`);
					break;
				}
				const siblings = Array.from(parent.children).filter((child) => child.tagName === current.tagName);
				segments.push(`${tag}[${Math.max(siblings.indexOf(current), 0)}]`);
				current = parent;
			}
			return segments.reverse().join('/');
		}

		function getAttributes(el) {
			const attrs = {};
			for (const attr of el.attributes) {
				if (!attr.value) continue;
				const attrName = (attr.name || '').toLowerCase();
				if (
					ATTRIBUTE_WHITELIST.has(attrName)
					|| attrName.includes('url')
					|| attrName.includes('href')
					|| attrName.includes('link')
				) {
					attrs[attr.name] = cleanText(attr.value);
				}
			}
			return attrs;
		}

		function getLabel(el) {
			const values = [
				el.innerText,
				el.textContent,
				el.getAttribute('aria-label'),
				el.getAttribute('title'),
				el.getAttribute('placeholder'),
				el.getAttribute('value'),
				el.getAttribute('alt'),
			];
			for (const value of values) {
				const cleaned = cleanText(value || '');
				if (cleaned) return cleaned;
			}
			return '';
		}

		function parseJsonLike(value) {
			const raw = cleanText(value || '');
			if (!raw) return null;
			try {
				return JSON.parse(raw);
			} catch (_error) {
				try {
					return JSON.parse(raw.replace(/&quot;/g, '"'));
				} catch (_nestedError) {
					return null;
				}
			}
		}

		function getActionUrl(el) {
			const directCandidates = [
				el.getAttribute('href'),
				el.getAttribute('data-url'),
				el.getAttribute('data-href'),
				el.getAttribute('data-link'),
				el.getAttribute('data-permalink'),
				el.getAttribute('data-job-url'),
				el.getAttribute('data-action-url'),
			];
			for (const candidate of directCandidates) {
				const absolute = toAbsoluteUrl(candidate || '');
				if (absolute) return absolute;
			}

			const wrapperLink = parseJsonLike(el.getAttribute('data-ep-wrapper-link'));
			if (wrapperLink && typeof wrapperLink === 'object') {
				const absolute = toAbsoluteUrl(wrapperLink.url || '');
				if (absolute) return absolute;
			}

			for (const attr of Array.from(el.attributes || [])) {
				const attrName = (attr.name || '').toLowerCase();
				if (
					!attrName.startsWith('data-')
					|| (!attrName.includes('url') && !attrName.includes('href') && !attrName.includes('link'))
				) {
					continue;
				}

				const parsed = parseJsonLike(attr.value);
				if (parsed && typeof parsed === 'object') {
					const absolute = toAbsoluteUrl(parsed.url || parsed.href || parsed.link || '');
					if (absolute) return absolute;
				}

				const absolute = toAbsoluteUrl(attr.value || '');
				if (absolute) return absolute;
			}

			const onClick = cleanText(el.getAttribute('onclick') || '');
			if (onClick) {
				const match = onClick.match(/(?:location|window\.location)(?:\.href)?\s*=\s*['"]([^'"]+)['"]/i)
					|| onClick.match(/window\.open\(\s*['"]([^'"]+)['"]/i);
				if (match) {
					const absolute = toAbsoluteUrl(match[1] || '');
					if (absolute) return absolute;
				}
			}

			return '';
		}

		function isInteractive(el) {
			const tag = el.tagName.toLowerCase();
			const role = (el.getAttribute('role') || '').toLowerCase();
			return ['a', 'button', 'input', 'select', 'textarea', 'summary'].includes(tag)
				|| Boolean(getActionUrl(el))
				|| el.hasAttribute('onclick')
				|| ['button', 'link', 'textbox', 'checkbox', 'radio', 'tab', 'menuitem', 'combobox', 'switch'].includes(role);
		}

		function inferInteractiveKind(el) {
			const tag = el.tagName.toLowerCase();
			const role = (el.getAttribute('role') || '').toLowerCase();
			const hasPopup = (el.getAttribute('aria-haspopup') || '').toLowerCase();
			const expanded = el.getAttribute('aria-expanded');
			const className = typeof el.className === 'string' ? el.className.toLowerCase() : '';
			const label = getLabel(el).toLowerCase();

			if (tag === 'a' || role === 'link' || Boolean(getActionUrl(el))) return 'link';
			if (tag === 'select' || role === 'combobox') return 'dropdown';
			if (tag === 'summary') return 'disclosure';
			if (tag === 'button' && ['listbox', 'menu', 'tree', 'dialog', 'grid'].includes(hasPopup)) return 'dropdown';
			if (tag === 'button' && (className.includes('dropdown') || className.includes('select') || label.includes('filter') || label.includes('sort'))) return 'dropdown';
			if (hasPopup === 'menu' || role === 'menuitem') return 'menu-trigger';
			if (className.includes('hamburger') || className.includes('menu-toggle') || label === 'menu') return 'hamburger';
			if (expanded !== null) return 'toggle';
			if (tag === 'input') {
				const inputType = (el.getAttribute('type') || 'text').toLowerCase();
				return ['checkbox', 'radio'].includes(inputType) ? inputType : 'input';
			}
			if (tag === 'textarea') return 'textarea';
			return 'button';
		}

		function registerInteractive(el, domPath) {
			const kind = inferInteractiveKind(el);
			const nodeId = stableId(`frame-0:${domPath}`);
			if (!selectorMap[String(nodeId)]) {
				const actionUrl = getActionUrl(el);
				selectorMap[String(nodeId)] = {
					node_id: nodeId,
					tag: el.tagName.toLowerCase(),
					kind,
					label: getLabel(el),
					attributes: getAttributes(el),
					dom_path: domPath,
					frame_index: 0,
					frame_url: window.location.href,
					parent_id: null,
					interactive: true,
					expanded: el.hasAttribute('aria-expanded') ? el.getAttribute('aria-expanded') === 'true' : null,
					has_popup: cleanText(el.getAttribute('aria-haspopup') || '') || null,
					action_url: actionUrl || null,
					is_link: kind === 'link',
					is_button: kind !== 'link',
				};
			}
			return nodeId;
		}

		function formatInteractive(el, domPath) {
			const kind = inferInteractiveKind(el);
			const label = escapeMarkdown(getLabel(el) || el.tagName.toLowerCase());
			const actionUrl = getActionUrl(el);
			if (kind === 'link') {
				const href = escapeMarkdownUrl(actionUrl);
				return href ? `[${label}](${href})` : label;
			}
			const nodeId = registerInteractive(el, domPath);
			return `[${kind}#${nodeId}] ${label}`;
		}

		function isSemanticBlock(el) {
			return ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'blockquote', 'pre', 'ul', 'ol', 'table'].includes(el.tagName.toLowerCase());
		}

		function hasSemanticAncestor(el) {
			let current = el.parentElement;
			while (current instanceof Element) {
				if (isSemanticBlock(current)) return true;
				current = current.parentElement;
			}
			return false;
		}

		function hasInteractiveAncestor(el) {
			let current = el.parentElement;
			while (current instanceof Element) {
				if (isInteractive(current)) return true;
				current = current.parentElement;
			}
			return false;
		}

		function hasListAncestor(el) {
			let current = el.parentElement;
			while (current instanceof Element) {
				const tag = current.tagName.toLowerCase();
				if (tag === 'li' || tag === 'ul' || tag === 'ol') return true;
				current = current.parentElement;
			}
			return false;
		}

		function formatAnchorForList(anchor) {
			const href = escapeMarkdownUrl(toAbsoluteUrl(anchor.getAttribute('href') || ''));
			if (!href) return '';
			const label = cleanText(getLabel(anchor));
			if (!label || label === href) return `[link](${href})`;
			return `[${escapeMarkdown(label)}](${href})`;
		}

		function renderList(el) {
			const isOrdered = el.tagName.toLowerCase() === 'ol';
			const lines = [];
			let index = 1;
			for (const item of Array.from(el.children)) {
				if (!(item instanceof HTMLLIElement) || !isVisible(item)) continue;
				const itemLines = cleanBlockText(item.innerText || item.textContent || '').split('\n').map((line) => line.trim()).filter(Boolean);
				if (!itemLines.length) continue;
				const prefix = isOrdered ? `${index}. ` : '- ';
				lines.push(`${prefix}${escapeMarkdown(itemLines[0])}`);
				for (const extra of itemLines.slice(1)) lines.push(`  ${escapeMarkdown(extra)}`);

				const itemLinks = Array.from(item.querySelectorAll('a[href]'))
					.map((anchor) => formatAnchorForList(anchor))
					.filter(Boolean);
				const seenLinks = new Set();
				for (const linkLine of itemLinks) {
					if (seenLinks.has(linkLine)) continue;
					seenLinks.add(linkLine);
					lines.push(`  ${linkLine}`);
				}
				index += 1;
			}
			return lines;
		}

		function renderTable(el) {
			const rows = Array.from(el.querySelectorAll('tr'))
				.map((row) =>
					Array.from(row.querySelectorAll('th, td'))
						.map((cell) => escapeMarkdown(cleanText(cell.innerText || cell.textContent || '')))
						.filter(Boolean)
				)
				.filter((row) => row.length > 0);
			if (!rows.length) return [];
			if (rows.length === 1) return [`| ${rows[0].join(' | ')} |`];

			const lines = [
				`| ${rows[0].join(' | ')} |`,
				`| ${rows[0].map(() => '---').join(' | ')} |`,
			];
			for (const row of rows.slice(1)) lines.push(`| ${row.join(' | ')} |`);
			return lines;
		}

		function renderBlock(el, domPath) {
			const tag = el.tagName.toLowerCase();
			if (isInteractive(el)) {
				const lines = [formatInteractive(el, domPath)];
				const textLines = cleanBlockText(el.innerText || el.textContent || '').split('\n').map((line) => line.trim()).filter(Boolean);
				const firstLabel = cleanText(getLabel(el));
				for (const extra of textLines) {
					if (cleanText(extra) !== firstLabel) lines.push(`  ${escapeMarkdown(extra)}`);
				}
				return lines;
			}
			if (tag === 'ul' || tag === 'ol') return renderList(el);
			if (tag === 'table') return renderTable(el);

			const lines = cleanBlockText(el.innerText || el.textContent || '').split('\n').map((line) => line.trim()).filter(Boolean);
			if (!lines.length) return [];
			if (/^h[1-6]$/.test(tag)) {
				lines[0] = `${'#'.repeat(Number(tag.slice(1)))} ${escapeMarkdown(lines[0])}`;
				for (let index = 1; index < lines.length; index += 1) lines[index] = escapeMarkdown(lines[index]);
				return lines;
			}
			if (tag === 'blockquote') return lines.map((line) => `> ${escapeMarkdown(line)}`);
			if (tag === 'pre') return ['```', ...lines.map((line) => escapeMarkdown(line)), '```'];
			return lines.map((line) => escapeMarkdown(line));
		}

		function collectCandidates(root) {
			const selector = [
				'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
				'p', 'blockquote', 'pre', 'ul', 'ol', 'table',
				'a', 'button', 'input', 'select', 'textarea', 'summary',
				'[role="button"]', '[role="combobox"]', '[role="link"]',
				'[data-url]', '[data-href]', '[data-link]', '[data-permalink]',
				'[data-job-url]', '[data-action-url]', '[data-ep-wrapper-link]', '[onclick]'
			].join(',');
			return Array.from(root.querySelectorAll(selector)).filter((el) => isVisible(el) && isAllowedSection(el));
		}

		const root = document.body || document.documentElement;
		const candidates = [];
		const seenPaths = new Set();

		for (const el of collectCandidates(root)) {
			if (isSemanticBlock(el) && hasSemanticAncestor(el)) continue;
			if (!isInteractive(el) && hasInteractiveAncestor(el)) continue;
			if (isInteractive(el) && hasListAncestor(el)) continue;

			const domPath = buildDomPath(el);
			if (seenPaths.has(domPath)) continue;
			seenPaths.add(domPath);

			const rect = el.getBoundingClientRect();
			candidates.push({
				element: el,
				domPath,
				top: rect.top,
				left: rect.left,
				height: rect.height,
			});
		}

		candidates.sort((a, b) => Math.abs(a.top - b.top) > 6 ? a.top - b.top : a.left - b.left);

		const lines = [`Page URL: ${window.location.href}`, ''];
		let previousTop = null;
		let previousRendered = '';

		for (const candidate of candidates) {
			const renderedLines = renderBlock(candidate.element, candidate.domPath);
			if (!renderedLines.length) continue;
			const renderedText = renderedLines.join('\n');
			if (renderedText === previousRendered) continue;

			if (previousTop !== null && Math.abs(candidate.top - previousTop) > Math.max(18, candidate.height * 0.4)) {
				lines.push('');
			}
			lines.push(...renderedLines);
			previousTop = candidate.top;
			previousRendered = renderedText;
		}

		return {
			page_url: window.location.href,
			content: lines.join('\n').replace(/\n{3,}/g, '\n\n').trim(),
			selector_map: selectorMap,
		};
	}
	"""
	return script
