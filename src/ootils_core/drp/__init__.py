"""
GELÉ (2026-07-19) — jamais branché en production ; candidat de réactivation :
DRP multi-échelon (per-site → central, ADR-020) ; ne pas compter en couverture.
Aucun chemin servi n'importe ootils_core.drp (seul test_drp_models le référence).
Vérifié encore mort au chantier moteur-c7 (la descente MRP ne l'a pas réveillé).
Voir docs/CARTE-CODE.md.

DRP (Distribution Requirements Planning) module.

Provides distribution network modeling including locations, links, and transportation lanes
for multi-echelon distribution planning.
"""
from .models import DistributionLink, TransportationLane, DistributionLinkEdge, LaneRequiresLinkEdge

__all__ = [
    "DistributionLink",
    "TransportationLane",
    "DistributionLinkEdge",
    "LaneRequiresLinkEdge",
]
