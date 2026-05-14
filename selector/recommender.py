import logging

logger = logging.getLogger(__name__)


def recommend_exclusions(manifest):

    recommendations = []

    for name, col in manifest.items():
        if col.density_pct == 0.0:
            logger.debug("EXCLUDE_CANDIDATE density=0%%: %s", name)

            recommendations.append(name)

        elif col.density_pct < 5.0:
            logger.debug("EXCLUDE_CANDIDATE density=%.1f%%: %s", col.density_pct, name)
            recommendations.append(name)

    return recommendations
