from __future__ import annotations

import hashlib
from pathlib import Path


TARGET = Path("/app/flaresolverr_service.py")
EXPECTED_SHA256 = "aeb870a59eb6ac868af5eec54f4ce9d0652a9b50b1c832507555cf62d04caecb"

OLD = '''def _get_turnstile_token(driver: WebDriver, tabs: int):
    token_input = driver.find_element(By.CSS_SELECTOR, "input[name='cf-turnstile-response']")
    current_value = token_input.get_attribute("value")
    while True:
        click_verify(driver, num_tabs=tabs)
        turnstile_token = token_input.get_attribute("value")
        if turnstile_token:
            if turnstile_token != current_value:
                logging.info(f"Turnstile token: {turnstile_token}")
                return turnstile_token
        logging.debug(f"Failed to extract token possibly click failed")        

        # reset focus
        driver.execute_script("""
            let el = document.createElement('button');
            el.style.position='fixed';
            el.style.top='0';
            el.style.left='0';
            document.body.prepend(el);
            el.focus();
        """)
        time.sleep(1)
'''

NEW = '''def _get_turnstile_token(driver: WebDriver, tabs: int):
    selector = "input[name='cf-turnstile-response']"
    token_input = driver.find_element(By.CSS_SELECTOR, selector)
    current_value = token_input.get_attribute("value")
    while True:
        click_verify(driver, num_tabs=tabs)
        token_inputs = driver.find_elements(By.CSS_SELECTOR, selector)
        if not token_inputs:
            logging.info("Turnstile challenge navigation completed")
            return ""
        turnstile_token = token_inputs[0].get_attribute("value")
        if turnstile_token and turnstile_token != current_value:
            logging.info("Turnstile token acquired")
            return turnstile_token
        logging.debug("Failed to extract token; challenge remains visible")

        # reset focus
        driver.execute_script("""
            let el = document.createElement('button');
            el.style.position='fixed';
            el.style.top='0';
            el.style.left='0';
            document.body.prepend(el);
            el.focus();
        """)
        time.sleep(1)
'''


payload = TARGET.read_bytes()
digest = hashlib.sha256(payload).hexdigest()
if digest != EXPECTED_SHA256:
    raise SystemExit(f"unsupported FlareSolverr source hash: {digest}")

text = payload.decode("utf-8")
if text.count(OLD) != 1:
    raise SystemExit("FlareSolverr Turnstile function did not match v3.5.0")
TARGET.write_text(text.replace(OLD, NEW), encoding="utf-8")
