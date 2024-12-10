from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import json
import time


def init_driver():
    """Inicializa o WebDriver com as configurações necessárias."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Modo sem interface gráfica
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def extract_marker_info(driver):
    """Extrai informações dos marcadores do mapa."""
    try:
        # Espera os elementos dos marcadores carregarem
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".leaflet-marker-icon"))
        )

        # Captura os marcadores visíveis
        markers = driver.find_elements(By.CSS_SELECTOR, ".leaflet-marker-icon")
        marker_data = []

        for marker in markers:
            # Obtém informações das classes e estilos
            marker_classes = marker.get_attribute("class")
            marker_style = marker.get_attribute("style")

            # Adiciona os dados capturados à lista
            marker_data.append({
                "classes": marker_classes,
                "style": marker_style,
                "popup_text": extract_popup_text(driver, marker)  # Captura texto do popup
            })

        return marker_data
    except Exception as e:
        print("Erro ao capturar dados dos marcadores:", e)
        return []


def extract_popup_text(driver, marker):
    """Clica no marcador e captura o texto do popup."""
    try:
        marker.click()  # Simula o clique no marcador
        time.sleep(2)  # Aguarda o popup aparecer

        # Captura o texto do popup
        popup = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".leaflet-popup-content"))
        )
        return popup.text
    except Exception as e:
        print("Erro ao capturar texto do popup:", e)
        return None


def save_data_as_json(data, file_name="markers.json"):
    """Salva os dados extraídos em um arquivo JSON."""
    try:
        with open(file_name, "w") as file:
            json.dump(data, file, indent=4)
        print(f"Dados salvos em {file_name}")
    except Exception as e:
        print("Erro ao salvar dados em JSON:", e)


def main():
    # URL do mapa
    url = "https://www.waze.com/livemap"  # Substitua com a URL real
    driver = init_driver()

    try:
        print(f"Acessando {url}...")
        driver.get(url)
        
        # Aguarda carregamento inicial
        time.sleep(5)
        
        # Extrai informações dos marcadores
        marker_data = extract_marker_info(driver)

        if marker_data:
            print(f"{len(marker_data)} marcadores encontrados.")
            for idx, data in enumerate(marker_data, start=1):
                print(f"Marcador {idx}:")
                print(f"  Classes: {data['classes']}")
                print(f"  Estilo: {data['style']}")
                print(f"  Texto do Popup: {data['popup_text']}")

            # Salva os dados em JSON
            save_data_as_json(marker_data)
        else:
            print("Nenhum marcador encontrado.")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
