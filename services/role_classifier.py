"""Enhanced role classification system for dealership contact finder."""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SeniorityLevel(Enum):
    C_SUITE = "C-Suite"
    SENIOR_EXECUTIVE = "Senior Executive"
    DIRECTOR = "Director"
    MANAGER = "Manager"
    SPECIALIST = "Specialist"
    COORDINATOR = "Coordinator"
    OTHER = "Other"


class RoleCategory(Enum):
    OWNERSHIP = "Ownership"
    SENIOR_LEADERSHIP = "Senior Leadership"
    MANAGEMENT = "Management"
    DEPARTMENT_HEAD = "Department Head"
    SPECIALIST = "Specialist"
    SALES = "Sales"
    SERVICE = "Service"
    FINANCE = "Finance"
    MARKETING = "Marketing"
    OPERATIONS = "Operations"
    IT_TECHNOLOGY = "IT/Technology"
    HR_ADMIN = "HR/Admin"
    OTHER = "Other"


@dataclass
class RoleClassification:
    category: RoleCategory
    seniority: SeniorityLevel
    confidence: float
    normalized_title: str
    keywords_matched: list[str]
    dealership_specific: bool


@dataclass
class RoleFilterCriteria:
    categories: Optional[list[RoleCategory]] = None
    seniority_levels: Optional[list[SeniorityLevel]] = None
    min_seniority_score: float = 0.0
    dealership_specific_only: bool = False
    exclude_categories: Optional[list[RoleCategory]] = None


class RoleClassifier:
    """Advanced role classification for dealership contacts."""

    def __init__(self):
        self._initialize_role_patterns()

    def _initialize_role_patterns(self):
        self.c_suite_patterns = {
            "ceo": ["chief executive officer", "ceo", "chief executive", "president & ceo"],
            "cfo": ["chief financial officer", "cfo", "chief financial"],
            "coo": ["chief operating officer", "coo", "chief operating"],
            "president": ["president", "company president"],
            "owner": ["owner", "co-owner", "business owner", "dealership owner"],
            "principal": ["principal", "managing principal"],
            "partner": ["managing partner", "partner", "equity partner"],
        }

        self.senior_executive_patterns = {
            "vice_president": [
                "vice president",
                "vp",
                "executive vice president",
                "evp",
                "senior vice president",
                "svp",
            ],
            "executive_director": ["executive director", "managing director"],
            "general_manager": ["general manager", "gm", "dealership general manager"],
            "dealer_principal": ["dealer principal", "dealer"],
            "senior_partner": ["senior partner"],
        }

        self.director_patterns = {
            "sales_director": ["sales director", "director of sales", "sales and leasing director"],
            "service_director": ["service director", "director of service", "fixed operations director"],
            "parts_director": ["parts director", "director of parts", "parts and service director"],
            "finance_director": ["finance director", "director of finance", "f&i director"],
            "marketing_director": ["marketing director", "director of marketing"],
            "operations_director": ["operations director", "director of operations"],
            "hr_director": ["hr director", "director of human resources", "people director"],
            "it_director": ["it director", "director of it", "technology director"],
        }

        self.management_patterns = {
            "sales_manager": [
                "sales manager",
                "new car sales manager",
                "used car sales manager",
                "leasing manager",
                "fleet sales manager",
                "internet sales manager",
            ],
            "service_manager": [
                "service manager",
                "fixed operations manager",
                "service operations manager",
                "warranty manager",
                "shop manager",
            ],
            "parts_manager": ["parts manager", "parts and accessories manager", "parts department manager"],
            "finance_manager": [
                "finance manager",
                "f&i manager",
                "business manager",
                "finance and insurance manager",
            ],
            "marketing_manager": ["marketing manager", "advertising manager", "digital marketing manager"],
            "hr_manager": ["hr manager", "human resources manager", "personnel manager"],
            "it_manager": ["it manager", "systems manager", "technology manager"],
            "operations_manager": ["operations manager", "facility manager", "admin manager"],
            "customer_relations_manager": [
                "customer relations manager",
                "customer service manager",
                "crm manager",
            ],
            "inventory_manager": ["inventory manager", "lot manager", "vehicle inventory manager"],
        }

        self.specialist_patterns = {
            "sales_specialist": [
                "sales consultant",
                "sales associate",
                "sales specialist",
                "product specialist",
                "senior sales consultant",
                "leasing specialist",
                "fleet specialist",
            ],
            "service_specialist": [
                "service advisor",
                "service consultant",
                "service specialist",
                "technical specialist",
                "warranty specialist",
            ],
            "parts_specialist": ["parts specialist", "parts advisor", "parts consultant", "parts counter"],
            "finance_specialist": ["finance specialist", "f&i specialist", "credit specialist", "lease specialist"],
            "marketing_specialist": ["marketing specialist", "marketing coordinator", "digital specialist"],
            "it_specialist": ["it specialist", "systems analyst", "tech specialist"],
            "customer_service_specialist": ["customer service specialist", "customer care specialist"],
        }

        self.coordinator_patterns = {
            "coordinator": ["coordinator", "assistant coordinator", "program coordinator"],
            "assistant": ["assistant", "administrative assistant", "executive assistant"],
            "receptionist": ["receptionist", "front desk", "customer service representative"],
            "clerk": ["clerk", "office clerk", "data entry clerk"],
            "trainee": ["trainee", "intern", "apprentice"],
        }

        self.dealership_keywords = {
            "new_car",
            "used_car",
            "pre-owned",
            "certified",
            "leasing",
            "financing",
            "service",
            "parts",
            "accessories",
            "warranty",
            "collision",
            "body_shop",
            "dealership",
            "automotive",
            "dealer",
            "showroom",
            "lot",
            "inventory",
            "trade_in",
            "appraisal",
            "f&i",
            "finance_and_insurance",
        }

        self.negative_patterns = {
            "intern",
            "student",
            "temp",
            "temporary",
            "contractor",
            "freelance",
            "volunteer",
            "part_time",
            "seasonal",
        }

    def classify_role(self, title: str, company_name: str = "") -> RoleClassification:
        """Classify a job title into role category and seniority level."""
        if not title or not isinstance(title, str):
            return self._create_other_classification("", 0.0)

        normalized = self._normalize_title(title)
        original_title = title.strip()

        if self._has_negative_patterns(normalized):
            return RoleClassification(
                category=RoleCategory.OTHER,
                seniority=SeniorityLevel.OTHER,
                confidence=0.2,
                normalized_title=original_title,
                keywords_matched=[],
                dealership_specific=False,
            )

        classification_attempts = [
            (self.c_suite_patterns, SeniorityLevel.C_SUITE),
            (self.senior_executive_patterns, SeniorityLevel.SENIOR_EXECUTIVE),
            (self.director_patterns, SeniorityLevel.DIRECTOR),
            (self.management_patterns, SeniorityLevel.MANAGER),
            (self.specialist_patterns, SeniorityLevel.SPECIALIST),
            (self.coordinator_patterns, SeniorityLevel.COORDINATOR),
        ]

        best_match = None
        best_confidence = 0.0

        for patterns_dict, seniority in classification_attempts:
            match_result = self._find_best_pattern_match(normalized, patterns_dict)
            if match_result and match_result["confidence"] > best_confidence:
                best_match = {
                    "seniority": seniority,
                    "category": self._determine_category(match_result["role_type"], seniority),
                    "confidence": match_result["confidence"],
                    "keywords": match_result["keywords"],
                }
                best_confidence = match_result["confidence"]

        if best_match:
            dealership_specific = self._is_dealership_specific(normalized) or self._is_dealership_company(company_name)

            if dealership_specific:
                best_match["confidence"] = min(1.0, best_match["confidence"] + 0.1)

            return RoleClassification(
                category=best_match["category"],
                seniority=best_match["seniority"],
                confidence=best_match["confidence"],
                normalized_title=original_title,
                keywords_matched=best_match["keywords"],
                dealership_specific=dealership_specific,
            )

        return self._create_fallback_classification(original_title, normalized, company_name)

    def _normalize_title(self, title: str) -> str:
        normalized = re.sub(r"\s+", " ", title.lower().strip())
        noise_words = ["the", "a", "an", "of", "and", "&", "at", "for", "in", "on"]
        words = normalized.split()
        filtered_words = [w for w in words if w not in noise_words]

        abbreviations = {
            "mgr": "manager",
            "dir": "director",
            "coord": "coordinator",
            "asst": "assistant",
            "sr": "senior",
            "jr": "junior",
            "vp": "vice president",
            "svp": "senior vice president",
            "evp": "executive vice president",
            "gm": "general manager",
        }

        expanded_words = []
        for word in filtered_words:
            clean_word = re.sub(r"[^\w]", "", word)
            expanded_words.append(abbreviations.get(clean_word, clean_word))

        return " ".join(expanded_words)

    def _find_best_pattern_match(self, normalized_title: str, patterns_dict: dict[str, list[str]]) -> Optional[dict]:
        best_match = None
        best_score = 0.0

        for role_type, patterns in patterns_dict.items():
            for pattern in patterns:
                score = self._calculate_pattern_match_score(normalized_title, pattern)
                if score > best_score:
                    best_score = score
                    best_match = {
                        "role_type": role_type,
                        "confidence": score,
                        "keywords": pattern.split(),
                        "pattern": pattern,
                    }

        return best_match if best_score > 0.3 else None

    def _calculate_pattern_match_score(self, title: str, pattern: str) -> float:
        title_words = set(title.split())
        pattern_words = set(pattern.split())

        if pattern == title:
            return 1.0
        if pattern in title:
            return 0.9

        if not pattern_words:
            return 0.0

        intersection = title_words.intersection(pattern_words)
        overlap_ratio = len(intersection) / len(pattern_words)

        key_words = {"manager", "director", "president", "ceo", "owner", "vice", "chief"}
        key_word_bonus = sum(0.1 for word in intersection if word in key_words)

        base_score = overlap_ratio + key_word_bonus
        penalty = len(title_words - pattern_words) * 0.05

        return max(0.0, min(1.0, base_score - penalty))

    def _determine_category(self, role_type: str, seniority: SeniorityLevel) -> RoleCategory:
        if any(keyword in role_type for keyword in ["owner", "principal", "partner"]):
            return RoleCategory.OWNERSHIP

        if seniority in [SeniorityLevel.C_SUITE, SeniorityLevel.SENIOR_EXECUTIVE]:
            return RoleCategory.SENIOR_LEADERSHIP

        category_mapping = {
            "sales": RoleCategory.SALES,
            "service": RoleCategory.SERVICE,
            "parts": RoleCategory.SERVICE,
            "finance": RoleCategory.FINANCE,
            "marketing": RoleCategory.MARKETING,
            "operations": RoleCategory.OPERATIONS,
            "hr": RoleCategory.HR_ADMIN,
            "it": RoleCategory.IT_TECHNOLOGY,
            "customer": RoleCategory.SALES,
        }

        for keyword, category in category_mapping.items():
            if keyword in role_type:
                if seniority == SeniorityLevel.DIRECTOR:
                    return RoleCategory.DEPARTMENT_HEAD
                elif seniority == SeniorityLevel.MANAGER:
                    return RoleCategory.MANAGEMENT
                elif seniority == SeniorityLevel.SPECIALIST:
                    return RoleCategory.SPECIALIST
                else:
                    return category

        if seniority == SeniorityLevel.DIRECTOR:
            return RoleCategory.DEPARTMENT_HEAD
        elif seniority == SeniorityLevel.MANAGER:
            return RoleCategory.MANAGEMENT
        elif seniority == SeniorityLevel.SPECIALIST:
            return RoleCategory.SPECIALIST

        return RoleCategory.OTHER

    def _is_dealership_specific(self, normalized_title: str) -> bool:
        title_words = set(normalized_title.split())
        return bool(title_words.intersection(self.dealership_keywords))

    def _is_dealership_company(self, company_name: str) -> bool:
        if not company_name:
            return False
        dealership_indicators = {
            "auto",
            "automotive",
            "car",
            "cars",
            "dealership",
            "dealer",
            "motors",
            "honda",
            "toyota",
            "ford",
            "chevrolet",
            "bmw",
            "mercedes",
            "audi",
            "nissan",
            "volkswagen",
            "hyundai",
            "kia",
            "mazda",
            "subaru",
            "lexus",
            "acura",
            "infiniti",
            "cadillac",
            "buick",
            "gmc",
        }
        company_lower = company_name.lower()
        return any(indicator in company_lower for indicator in dealership_indicators)

    def _has_negative_patterns(self, normalized_title: str) -> bool:
        return any(pattern in normalized_title for pattern in self.negative_patterns)

    def _create_other_classification(self, title: str, confidence: float) -> RoleClassification:
        return RoleClassification(
            category=RoleCategory.OTHER,
            seniority=SeniorityLevel.OTHER,
            confidence=confidence,
            normalized_title=title,
            keywords_matched=[],
            dealership_specific=False,
        )

    def _create_fallback_classification(
        self, original_title: str, normalized_title: str, company_name: str
    ) -> RoleClassification:
        dealership_specific = self._is_dealership_specific(normalized_title) or self._is_dealership_company(
            company_name
        )
        confidence = 0.3 if dealership_specific else 0.2

        if any(word in normalized_title for word in ["sales", "sell"]):
            category = RoleCategory.SALES
        elif any(word in normalized_title for word in ["service", "repair", "maintenance"]):
            category = RoleCategory.SERVICE
        elif any(word in normalized_title for word in ["finance", "accounting", "credit"]):
            category = RoleCategory.FINANCE
        else:
            category = RoleCategory.OTHER

        return RoleClassification(
            category=category,
            seniority=SeniorityLevel.OTHER,
            confidence=confidence,
            normalized_title=original_title,
            keywords_matched=[],
            dealership_specific=dealership_specific,
        )

    def get_seniority_score(self, seniority: SeniorityLevel) -> float:
        seniority_scores = {
            SeniorityLevel.C_SUITE: 1.0,
            SeniorityLevel.SENIOR_EXECUTIVE: 0.85,
            SeniorityLevel.DIRECTOR: 0.7,
            SeniorityLevel.MANAGER: 0.55,
            SeniorityLevel.SPECIALIST: 0.4,
            SeniorityLevel.COORDINATOR: 0.25,
            SeniorityLevel.OTHER: 0.1,
        }
        return seniority_scores.get(seniority, 0.1)

    def filter_contacts_by_role(self, contacts: list[dict], criteria: RoleFilterCriteria) -> list[dict]:
        """Filter contacts based on role criteria."""
        if not contacts or not criteria:
            return contacts

        filtered = []
        for contact in contacts:
            title = contact.get("title", "")
            company_name = contact.get("company_name", "")
            classification = self.classify_role(title, company_name)

            if self._contact_matches_criteria(classification, criteria):
                contact["role_classification"] = classification
                filtered.append(contact)

        return filtered

    def _contact_matches_criteria(self, classification: RoleClassification, criteria: RoleFilterCriteria) -> bool:
        if criteria.exclude_categories and classification.category in criteria.exclude_categories:
            return False
        if criteria.categories and classification.category not in criteria.categories:
            return False
        if criteria.seniority_levels and classification.seniority not in criteria.seniority_levels:
            return False
        if criteria.min_seniority_score > 0:
            if self.get_seniority_score(classification.seniority) < criteria.min_seniority_score:
                return False
        if criteria.dealership_specific_only and not classification.dealership_specific:
            return False
        return True

    def get_role_statistics(self, contacts: list[dict]) -> dict[str, Any]:
        """Generate role distribution statistics for a list of contacts."""
        if not contacts:
            return {}

        stats: dict[str, Any] = {
            "total_contacts": len(contacts),
            "category_distribution": {},
            "seniority_distribution": {},
            "dealership_specific_count": 0,
            "avg_role_confidence": 0.0,
            "top_titles": {},
            "seniority_scores": [],
        }

        category_counts = {category: 0 for category in RoleCategory}
        seniority_counts = {seniority: 0 for seniority in SeniorityLevel}
        confidence_sum = 0.0
        title_counts: dict[str, int] = {}

        for contact in contacts:
            title = contact.get("title", "")
            company_name = contact.get("company_name", "")
            classification = self.classify_role(title, company_name)

            category_counts[classification.category] += 1
            seniority_counts[classification.seniority] += 1
            confidence_sum += classification.confidence

            if classification.dealership_specific:
                stats["dealership_specific_count"] += 1

            if title:
                title_counts[title] = title_counts.get(title, 0) + 1

            stats["seniority_scores"].append(self.get_seniority_score(classification.seniority))

        total = len(contacts)
        stats["category_distribution"] = {
            category.value: {"count": count, "percentage": round((count / total) * 100, 1)}
            for category, count in category_counts.items()
            if count > 0
        }

        stats["seniority_distribution"] = {
            seniority.value: {"count": count, "percentage": round((count / total) * 100, 1)}
            for seniority, count in seniority_counts.items()
            if count > 0
        }

        stats["avg_role_confidence"] = round(confidence_sum / total, 2) if total > 0 else 0.0
        stats["dealership_specific_percentage"] = round((stats["dealership_specific_count"] / total) * 100, 1)
        stats["top_titles"] = dict(sorted(title_counts.items(), key=lambda x: x[1], reverse=True)[:10])

        if stats["seniority_scores"]:
            stats["avg_seniority_score"] = round(sum(stats["seniority_scores"]) / len(stats["seniority_scores"]), 2)
        else:
            stats["avg_seniority_score"] = 0.0

        return stats
