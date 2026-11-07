/**
 * Rich cards for Slack: a json-render spec (fixed component catalog: Card, Section, Text,
 * Fields/Field, Table, Chart, Divider, Image, Actions, Button, LinkButton) → native Block Kit.
 *
 * Buttons reuse the poll action-id contract (`poll:vote:<cardId>:<idx>`, see poll-blocks.ts),
 * so the host app records the click and reports it back like a directed question.
 */
import { toCard, standardComponentDefinitions } from '@json-render/chat';
import { cardToBlockKit, cardToFallbackText } from '@chat-adapter/slack';
import type { PollOption } from './poll-blocks.ts';

export interface RenderedCard {
  blocks: unknown[];
  text: string;
  /** Clickable (non-link) buttons, index-aligned with their `poll:vote` action ids. */
  options: PollOption[];
}

/** Catalog summary for the tool description, so the model only uses known components. */
export const CARD_COMPONENTS = Object.keys(standardComponentDefinitions).join(', ');

/** Throws CardSpecError (one readable issue per problem) on an invalid spec. */
export function renderCard(spec: unknown, cardId: string): RenderedCard {
  const card = toCard(spec as any);
  const blocks = cardToBlockKit(card) as any[];
  const options: PollOption[] = [];
  for (const b of blocks) {
    if (b?.type !== 'actions') continue;
    for (const el of b.elements ?? []) {
      if (el?.type !== 'button' || el.url) continue;
      const i = options.length;
      options.push({ label: String(el.text?.text ?? `Option ${i + 1}`) });
      el.action_id = `poll:vote:${cardId}:${i}`;
      el.value = String(i);
    }
  }
  return { blocks, text: cardToFallbackText(card), options };
}
