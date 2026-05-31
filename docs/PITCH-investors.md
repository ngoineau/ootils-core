# Ootils — Pitch investisseurs & lecture de marché

> **Statut** : support de pitch (draft) — 2026-05-31. Document business/interne.
> Ancré sur [`STRATEGY.md`](STRATEGY.md). Distingue ce qui est **livré** de ce
> qui est **en conception** (cf. [`PROJECT-STATUS.md`](PROJECT-STATUS.md) §2,
> [`ADR-019`](ADR-019-demand-model-pyramide.md)). Ne vaut pas engagement produit.

---

## La phrase
**« Une fonction supply chain complète, opérée par des agents IA, pour 20 % du coût d'une organisation traditionnelle. »**
*Pas une thèse « meilleur APS » — une thèse de transformation opérationnelle.*

## Elevator pitch (30 s)
Une supply chain coûte cher non à cause du logiciel, mais des **humains qui compensent ses lacunes** (planificateurs triant les exceptions, analystes nettoyant la donnée, contrôleurs validant les paramètres, consultants produisant des rapports). Ootils remplace ou augmente radicalement chacun de ces rôles par une **flotte d'agents IA spécialisés**, tournant sur un **moteur de décision déterministe** conçu pour être piloté par des agents — pas cliqué par des humains. Les agents proposent et diagnostiquent ; les humains supervisent les exceptions et valident l'irréversible.

---

## Le pitch (10 points)

1. **Problème** — La supply chain est le dernier grand centre de coût massivement manuel. Les APS (SAP IBP, o9, Kinaxis, Blue Yonder) automatisent le *calcul*, pas le *travail*. Le coût, c'est l'humain, pas la licence.
2. **Pourquoi maintenant** — Les agents IA peuvent absorber ce travail, mais un LLM seul ne peut pas piloter une supply chain (hallucination, pas d'explicabilité, pas de reproductibilité). Il lui faut un **substrat déterministe** — ce qu'aucun APS n'offre et ce que Ootils est conçu pour être.
3. **Solution** — Ootils = le **substrat** (graphe, propagation déterministe, forking de scénarios, explicabilité causale, audit) **+ la flotte** d'agents gouvernés (proposition L1 → validation humaine pour l'irréversible).
4. **Moat** — (a) 30 ans d'expertise opérationnelle = savoir *quoi* construire et dans quel ordre (le moteur ne s'achète pas) ; (b) première référence payante ; (c) communauté (open-core).
5. **Wedge** — « Tour de contrôle autonome des ruptures, avec recommandations adossées à des scénarios. » ROI immédiat, on étend ensuite à la flotte complète.
6. **Marché & client cible** — Industriel/distributeur $200M–2B, data stack moderne, frustré que son APS ne dialogue pas avec des agents IA. Budget $150–500K/an. **Beachhead** : distributeur à 325 DC × ~5 000 SKU, flux quotidiens (vitrine du *risk pooling* : safety central + DRP = moins de stock total).
7. **Business model** — Open-core (dbt/Airbyte/Grafana) : moteur ouvert (crédibilité + communauté) ; agents avancés, connecteurs ERP/WMS et service managé propriétaires.
8. **Traction (honnête)** — ✅ Moteur construit & testé (graphe, propagation, scénarios, explicabilité, MRP, moteur Rust). ✅ 5 agents watchers gouvernés livrés + tests CI. ✅ Module demande (Pyramide) entièrement conçu (ADR-019). 🔜 Validation sur données réelles. *Stade : preuve d'architecture sur données synthétiques, pré-revenu.*
9. **Équipe** — Fondateur : 30 ans de direction supply chain opérationnelle. Renfort technique identifié.
10. **Demande** — Fenêtre 12 mois : finaliser la flotte + livrer le 1ᵉʳ POC client, puis passer à l'échelle. Montant/structure : à définir.

---

## Lecture de marché (analyse honnête, à valider par recherche documentée)

**Verdict : oui, il y a un marché — mais deux marchés distincts.**

**Signaux pour :**
- Douleur réelle et chère (le coût est humain, pool plus gros que le budget logiciel).
- « Pourquoi maintenant » juste : tout le monde colle un LLM sur la SC et se cogne au mur hallucination/explicabilité ; le substrat déterministe est mal servi aujourd'hui.
- Willing-to-pay crédible ($150–500K/an) **si** réduction réelle de têtes/stock.
- Beachhead concret identifié (325 DC).

**Risques :**
- « 20 % du coût » = remplacer des planificateurs → organisationnellement dur, acheteurs risk-averse (la SC est critique). Vendre « augmentez/supprimez le travail ingrat » passe mieux que « licenciez ».
- Fenêtre étroite : un concurrent financé peut répliquer le moteur en 3–6 mois ; les incumbents ajoutent de l'IA maintenant. Moat réel mais non prouvé.
- Modèle « opéré par agents » jamais démontré à l'échelle = vision, pas encore fait.

**Ce qui tranche :** la vraie question n'est pas « y a-t-il un marché ? » (oui) mais « peux-tu le capturer avant les incumbents et prouver le modèle agent opérationnellement ? ». Le risque décisif est **go-to-market + adoption**, pas la demande. **Tout repose sur UN premier design partner payant** — tant qu'il n'a pas signé, le marché est une hypothèse forte, pas un fait.

**Recommandation de positionnement :** lever en menant avec **le wedge** (tour de contrôle ruptures qui *augmente* les planificateurs, ROI chiffrable), pas avec « remplacez votre supply chain ». Garder la vision « 20 % du coût » comme **upside** (la flotte complète une fois le wedge prouvé).

---

## À faire pour muscler le dossier
- [ ] Recherche de marché documentée avec sources (taille marché APS/planning, coût d'une fonction SC type, mouvements IA des incumbents, comparables financés).
- [ ] Chiffrer le ROI du wedge sur le beachhead (réduction de stock via pooling, ruptures évitées).
- [ ] Sécuriser un 1ᵉʳ design partner payant = le jalon qui transforme l'hypothèse en fait.
