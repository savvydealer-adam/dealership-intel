import functools
import logging
import random
import time
from typing import Any, Callable, Optional

import requests

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    backoff_base: float = 1,
    backoff_multiplier: float = 2,
    max_delay: float = 60,
    jitter: bool = True,
    retryable_status_codes: Optional[list[int]] = None,
):
    """Decorator for API methods that implements exponential backoff retry logic."""
    if retryable_status_codes is None:
        retryable_status_codes = [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527]

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(self, *args, **kwargs)

                except requests.exceptions.RequestException as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) reached for {func.__name__} - "
                            f"{type(e).__name__}: {str(e)[:200]}"
                        )
                        raise e

                    if hasattr(e, "response") and e.response is not None:
                        if e.response.status_code not in retryable_status_codes:
                            logger.info(
                                f"Non-retryable HTTP {e.response.status_code} error for {func.__name__} - "
                                f"{type(e).__name__}: {str(e)[:100]}"
                            )
                            raise e

                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after and e.response.status_code in [429, 503]:
                            try:
                                delay = min(float(retry_after), max_delay)
                                status_msg = "Rate limited" if e.response.status_code == 429 else "Service unavailable"
                                logger.info(
                                    f"{status_msg}, honoring Retry-After: {delay:.2f}s "
                                    f"(attempt {attempt + 1}/{max_retries + 1} for {func.__name__})"
                                )
                                time.sleep(delay)
                                continue
                            except ValueError:
                                logger.warning(
                                    f"Invalid Retry-After header value '{retry_after}', "
                                    "falling back to exponential backoff"
                                )

                    delay = min(backoff_base * (backoff_multiplier**attempt), max_delay)
                    if jitter:
                        delay = delay * random.random()

                    logger.info(
                        f"Retrying {func.__name__} in {delay:.2f}s "
                        f"(attempt {attempt + 1}/{max_retries + 1}, "
                        f"reason: {type(e).__name__}: {str(e)[:100]})"
                    )
                    time.sleep(delay)

                except ValueError as e:
                    if attempt == max_retries:
                        logger.error(f"JSON decode error after max retries for {func.__name__}: {str(e)[:200]}")
                        raise e

                    last_exception = e
                    delay = min(backoff_base * (backoff_multiplier**attempt), max_delay)
                    if jitter:
                        delay = delay * random.random()

                    logger.info(
                        f"Retrying {func.__name__} due to JSON error in {delay:.2f}s "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )
                    time.sleep(delay)

                except Exception as e:
                    logger.error(
                        f"Unexpected non-retryable error in {func.__name__} - {type(e).__name__}: {str(e)[:200]}"
                    )
                    raise e

            if last_exception:
                raise last_exception
            return None

        return wrapper

    return decorator


class ApolloAPIService:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.apollo.io/v1"
        self.session = requests.Session()
        self.session.headers.update({"X-Api-Key": self.api_key, "Content-Type": "application/json"})

    @retry_with_backoff(max_retries=3, backoff_base=1, backoff_multiplier=2)
    def search_company(self, domain: str, company_name: Optional[str] = None) -> Optional[dict[str, Any]]:
        """Search for company information using domain."""
        search_params: dict[str, Any] = {"organization_domains": [domain], "per_page": 1, "page": 1}

        if company_name:
            search_params["organization_name"] = company_name

        response = self.session.post(f"{self.base_url}/organizations/search", json=search_params, timeout=30)

        if not response.ok:
            response.raise_for_status()

        data = response.json()
        organizations = data.get("organizations", [])

        if organizations:
            company = organizations[0]
            return {
                "id": company.get("id"),
                "name": company.get("name"),
                "domain": company.get("primary_domain"),
                "website_url": company.get("website_url"),
                "industry": company.get("industry"),
                "estimated_num_employees": company.get("estimated_num_employees"),
                "phone": company.get("phone"),
                "address": self._format_address(company),
                "linkedin_url": company.get("linkedin_url"),
                "founded_year": company.get("founded_year"),
                "description": company.get("short_description"),
            }

        logger.info(f"No organizations found for domain: {domain}")
        return None

    def search_company_multi_strategy(
        self, domain: str, company_name: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        """Search for company using multiple strategies for better success rate."""
        logger.info(f"Starting multi-strategy search for domain: {domain}, company_name: {company_name}")

        # Strategy 1: Domain + Company Name
        if domain and company_name:
            logger.info(f"Strategy 1: Searching with domain '{domain}' + company name '{company_name}'")
            result = self.search_company(domain, company_name)
            if result:
                logger.info(f"Strategy 1 SUCCESS: Found company '{result.get('name')}'")
                return result

        # Strategy 2: Domain only
        if domain:
            logger.info(f"Strategy 2: Searching with domain '{domain}' only")
            result = self.search_company(domain)
            if result:
                logger.info(f"Strategy 2 SUCCESS: Found company '{result.get('name')}'")
                return result

        # Strategy 3: Company Name only
        if company_name:
            logger.info(f"Strategy 3: Searching with company name '{company_name}' only")
            result = self._search_by_name_only(company_name)
            if result:
                logger.info(f"Strategy 3 SUCCESS: Found company '{result.get('name')}'")
                return result

        # Strategy 4: Domain variations
        if domain:
            logger.info(f"Strategy 4: Trying domain variations for '{domain}'")
            variations = self._generate_domain_variations(domain)
            for variation in variations:
                result = self.search_company(variation, company_name)
                if result:
                    logger.info(f"Strategy 4 SUCCESS: Found '{result.get('name')}' with '{variation}'")
                    return result

        logger.info(f"All strategies failed for domain: {domain}, company_name: {company_name}")
        return None

    @retry_with_backoff(max_retries=3, backoff_base=1, backoff_multiplier=2)
    def _search_by_name_only(self, company_name: str) -> Optional[dict[str, Any]]:
        """Search for company by name only."""
        search_params: dict[str, Any] = {"organization_name": company_name, "per_page": 3, "page": 1}

        response = self.session.post(f"{self.base_url}/organizations/search", json=search_params, timeout=30)

        if not response.ok:
            response.raise_for_status()

        data = response.json()
        organizations = data.get("organizations", [])

        if organizations:
            company = organizations[0]
            return {
                "id": company.get("id"),
                "name": company.get("name"),
                "domain": company.get("primary_domain"),
                "website_url": company.get("website_url"),
                "industry": company.get("industry"),
                "estimated_num_employees": company.get("estimated_num_employees"),
                "phone": company.get("phone"),
                "address": self._format_address(company),
                "linkedin_url": company.get("linkedin_url"),
                "founded_year": company.get("founded_year"),
                "description": company.get("short_description"),
            }

        return None

    def _generate_domain_variations(self, domain: str) -> list[str]:
        """Generate common domain variations that might exist in Apollo."""
        variations = []

        if not domain.startswith("www."):
            variations.append(f"www.{domain}")
        else:
            variations.append(domain[4:])

        base_domain = domain.replace("www.", "")
        for sub in ["shop", "store", "sales", "main", "site"]:
            variations.append(f"{sub}.{base_domain}")

        if base_domain.endswith(".com"):
            base_no_tld = base_domain[:-4]
            variations.extend([f"{base_no_tld}.net", f"{base_no_tld}.org"])

        return variations[:5]

    @retry_with_backoff(max_retries=3, backoff_base=1, backoff_multiplier=2)
    def search_people(
        self,
        organization_id: Optional[str] = None,
        domain: Optional[str] = None,
        limit: int = 10,
        role_filter_criteria=None,
    ) -> list[dict[str, Any]]:
        """Search for people/management at a company with optional role filtering."""
        search_params: dict[str, Any] = {"per_page": min(limit, 25), "page": 1}

        if role_filter_criteria:
            apollo_seniorities, apollo_titles = self._convert_role_criteria_to_apollo_params(role_filter_criteria)
            if apollo_seniorities:
                search_params["person_seniorities"] = apollo_seniorities
            if apollo_titles:
                search_params["person_titles"] = apollo_titles
        else:
            search_params["person_seniorities"] = ["owner", "c_suite", "vp", "director", "manager"]
            search_params["person_titles"] = [
                "owner",
                "president",
                "ceo",
                "cfo",
                "general manager",
                "managing partner",
                "director",
                "vice president",
            ]

        if organization_id:
            search_params["organization_ids"] = [organization_id]
        elif domain:
            search_params["organization_domains"] = [domain]
        else:
            return []

        response = self.session.post(f"{self.base_url}/mixed_people/search", json=search_params, timeout=30)

        if not response.ok:
            response.raise_for_status()

        data = response.json()
        people = data.get("people", [])

        logger.info(f"Apollo API Response - Found {len(people)} people for org_id={organization_id}, domain={domain}")

        if not people:
            logger.info(f"No people found for organization_id={organization_id}, domain={domain}")

        return [self._format_person_data(person) for person in people]

    def _convert_role_criteria_to_apollo_params(self, role_filter_criteria) -> tuple:
        """Convert RoleFilterCriteria to Apollo API search parameters."""
        apollo_seniorities: set[str] = set()
        apollo_titles: set[str] = set()

        VALID_APOLLO_SENIORITIES = {"c_suite", "vp", "director", "manager", "owner"}

        if role_filter_criteria.seniority_levels:
            seniority_mapping = {
                "C-Suite": ["c_suite"],
                "Senior Executive": ["c_suite", "vp"],
                "Director": ["director"],
                "Manager": ["manager"],
                "Specialist": ["manager"],
                "Coordinator": [],
                "Other": [],
            }

            for seniority_level in role_filter_criteria.seniority_levels:
                apollo_levels = seniority_mapping.get(seniority_level.value, [])
                apollo_seniorities.update(apollo_levels)

        if role_filter_criteria.categories:
            category_titles = {
                "Ownership": [
                    "owner",
                    "co-owner",
                    "dealership owner",
                    "business owner",
                    "dealer principal",
                    "principal",
                    "managing partner",
                    "partner",
                ],
                "Senior Leadership": [
                    "ceo",
                    "president",
                    "chief executive officer",
                    "chief executive",
                    "managing director",
                    "executive director",
                    "general manager",
                    "dealership general manager",
                    "gm",
                ],
                "Management": [
                    "general manager",
                    "regional manager",
                    "district manager",
                    "dealership manager",
                    "location manager",
                    "branch manager",
                ],
                "Department Head": [
                    "director",
                    "vice president",
                    "vp",
                    "senior vice president",
                    "svp",
                    "executive vice president",
                    "evp",
                ],
                "Sales": [
                    "sales director",
                    "sales manager",
                    "general sales manager",
                    "new car manager",
                    "used car manager",
                    "pre-owned manager",
                    "fleet manager",
                    "commercial director",
                    "sales vp",
                ],
                "Service": [
                    "service director",
                    "service manager",
                    "fixed operations director",
                    "fixed operations manager",
                    "parts director",
                    "parts manager",
                    "warranty manager",
                    "collision center manager",
                ],
                "Finance": [
                    "cfo",
                    "finance director",
                    "finance manager",
                    "controller",
                    "f&i manager",
                    "business manager",
                    "finance and insurance manager",
                ],
                "Marketing": [
                    "marketing director",
                    "marketing manager",
                    "advertising director",
                    "digital marketing manager",
                    "brand manager",
                ],
                "Operations": [
                    "operations director",
                    "operations manager",
                    "facility manager",
                    "administrative manager",
                    "office manager",
                ],
                "IT/Technology": [
                    "it director",
                    "technology director",
                    "systems manager",
                    "it manager",
                    "digital operations manager",
                ],
                "HR/Admin": [
                    "hr director",
                    "human resources director",
                    "admin director",
                    "hr manager",
                    "human resources manager",
                    "people director",
                ],
            }

            for category in role_filter_criteria.categories:
                titles = category_titles.get(category.value, [])
                apollo_titles.update(titles)

        if role_filter_criteria.dealership_specific_only:
            dealership_specific_titles = [
                "dealer principal",
                "dealership owner",
                "dealership general manager",
                "dealer",
                "managing dealer",
                "dealer operator",
                "general sales manager",
                "new car sales manager",
                "used car sales manager",
                "pre-owned manager",
                "fleet sales manager",
                "internet sales manager",
                "leasing manager",
                "sales director",
                "service manager",
                "fixed operations manager",
                "parts manager",
                "service director",
                "fixed operations director",
                "parts director",
                "collision center manager",
                "body shop manager",
                "warranty manager",
                "f&i manager",
                "finance and insurance manager",
                "business manager",
                "finance manager",
                "credit manager",
                "dealership operations manager",
                "lot manager",
                "inventory manager",
                "facility manager",
                "administrative manager",
            ]
            apollo_titles.update(dealership_specific_titles)

        filtered_seniorities = [s for s in apollo_seniorities if s in VALID_APOLLO_SENIORITIES]
        return list(filtered_seniorities), list(apollo_titles)

    @retry_with_backoff(max_retries=3, backoff_base=1, backoff_multiplier=2)
    def enrich_person(self, email: str) -> Optional[dict[str, Any]]:
        """Enrich person data using email address."""
        response = self.session.post(f"{self.base_url}/people/match", json={"email": email}, timeout=30)

        if not response.ok:
            response.raise_for_status()

        data = response.json()
        person = data.get("person")

        if person:
            return self._format_person_data(person)

        logger.info(f"No person found for email: {email}")
        return None

    def _format_person_data(self, person: dict[str, Any]) -> dict[str, Any]:
        """Format person data from Apollo API response."""
        name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
        logger.debug(f"Processing person: {name}, title: {person.get('title')}, email: {person.get('email')}")

        return {
            "id": person.get("id"),
            "name": name,
            "first_name": person.get("first_name"),
            "last_name": person.get("last_name"),
            "title": person.get("title"),
            "email": person.get("email"),
            "phone": self._format_phone(person.get("phone_numbers", [])),
            "linkedin_url": person.get("linkedin_url"),
            "seniority": person.get("seniority"),
            "departments": ", ".join(person.get("departments", [])),
            "organization_name": person.get("organization", {}).get("name", ""),
            "source": "apollo",
        }

    def _format_address(self, company: dict[str, Any]) -> str:
        """Format company address from Apollo data."""
        parts = []
        for field in ["street_address", "city", "state", "postal_code", "country"]:
            if company.get(field):
                parts.append(company[field])
        return ", ".join(parts)

    def _format_phone(self, phone_numbers: list[dict[str, Any]]) -> str:
        """Format phone numbers from Apollo data."""
        if not phone_numbers:
            return ""
        phone = phone_numbers[0]
        return phone.get("sanitized_number", "") or phone.get("raw_number", "")

    def check_api_usage(self) -> dict[str, Any]:
        """Check API usage and limits."""
        try:
            response = self.session.get(f"{self.base_url}/auth/me", timeout=30)
            if response.ok:
                return response.json()
            else:
                logger.warning(f"API usage check failed with status {response.status_code}")
        except Exception as e:
            logger.error(f"Error checking API usage - {type(e).__name__}: {str(e)[:100]}")
        return {}
